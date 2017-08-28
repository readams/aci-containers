// Copyright 2017 Cisco Systems, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package hostagent

import (
	"encoding/json"
	"fmt"
	"errors"
	"io/ioutil"
	"net"
	"os"
	"reflect"
	"strings"
	"sync"
	"syscall"

	etcdclient "github.com/coreos/etcd/client"
	"github.com/coreos/go-iptables/iptables"
	"github.com/Sirupsen/logrus"
	"github.com/vishvananda/netlink"

	"k8s.io/client-go/kubernetes"
	restclient "k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/clientcmd"

	md "github.com/noironetworks/aci-containers/pkg/metadata"
	"github.com/noironetworks/aci-containers/pkg/etcd"
)

type Environment interface {
	Init(agent *HostAgent) error
	PrepareRun(stopCh <-chan struct{}) error

	CniDeviceChanged(metadataKey *string, id *md.ContainerId)
	CniDeviceDeleted(metadataKey *string, id *md.ContainerId)
}

type K8sEnvironment struct {
	kubeClient        *kubernetes.Clientset
	agent             *HostAgent
	podInformer       cache.SharedIndexInformer
	endpointsInformer cache.SharedIndexInformer
	serviceInformer   cache.SharedIndexInformer
	nodeInformer      cache.SharedIndexInformer
	log               *logrus.Logger
}

func NewK8sEnvironment(config *HostAgentConfig, log *logrus.Logger) (*K8sEnvironment, error) {
	if config.NodeName == "" {
		config.NodeName = os.Getenv("KUBERNETES_NODE_NAME")
	}
	if config.NodeName == "" {
		err := errors.New("Node name not specified and $KUBERNETES_NODE_NAME empty")
		log.Error(err.Error())
		return nil, err
	}

	log.WithFields(logrus.Fields{
		"kubeconfig":  config.KubeConfig,
		"node-name":   config.NodeName,
	}).Info("Setting up Kubernetes environment")

	log.Debug("Initializing kubernetes client")
	var restconfig *restclient.Config
	var err error
	if config.KubeConfig != "" {
		// use kubeconfig file from command line
		restconfig, err =
			clientcmd.BuildConfigFromFlags("", config.KubeConfig)
		if err != nil {
			return nil, err
		}
	} else {
		// creates the in-cluster config
		restconfig, err = restclient.InClusterConfig()
		if err != nil {
			return nil, err
		}
	}

	// creates the kubernetes API client
	kubeClient, err := kubernetes.NewForConfig(restconfig)
	if err != nil {
		return nil, err
	}
	return &K8sEnvironment{kubeClient: kubeClient, log: log}, nil
}

func (env *K8sEnvironment) Init(agent *HostAgent) error {
	env.agent = agent
	
	env.log.Debug("Initializing informers")
    env.agent.initNodeInformerFromClient(env.kubeClient)
	env.agent.initPodInformerFromClient(env.kubeClient)
	env.agent.initEndpointsInformerFromClient(env.kubeClient)
	env.agent.initServiceInformerFromClient(env.kubeClient)
	return nil
}

func (env *K8sEnvironment) PrepareRun(stopCh <-chan struct{}) error {
	env.log.Debug("Starting node informer")
	go env.agent.nodeInformer.Run(stopCh)

	env.log.Info("Waiting for node cache sync")
	cache.WaitForCacheSync(stopCh, env.agent.nodeInformer.HasSynced)
	env.log.Info("Node cache sync successful")

	env.log.Debug("Starting remaining informers")
	go env.agent.podInformer.Run(stopCh)
	go env.agent.endpointsInformer.Run(stopCh)
	go env.agent.serviceInformer.Run(stopCh)

	env.log.Info("Waiting for cache sync for remaining objects")
	cache.WaitForCacheSync(stopCh,
		env.agent.podInformer.HasSynced, env.agent.endpointsInformer.HasSynced,
		env.agent.serviceInformer.HasSynced)
	env.log.Info("Cache sync successful")
	return nil
}

func (env *K8sEnvironment) CniDeviceChanged(metadataKey *string, id *md.ContainerId) {
	env.agent.podChanged(metadataKey)
}

func (env *K8sEnvironment) CniDeviceDeleted(metadataKey *string, id *md.ContainerId) {
}

const (
	NAT_PRE_CHAIN = "aci-nat-pre"
	NAT_POST_CHAIN = "aci-nat-post"
)

type CfEnvironment struct {
	cellID       string
	agent        *HostAgent
	cfconfig     *CfConfig
	etcdClient   etcdclient.Client

	indexLock    sync.Locker
	epIdx        map[string]etcd.EpInfo

	iptbl        *iptables.IPTables
	ctPortMap    map[string]map[uint32]uint32
	cfNetv4      bool
	cfNetLink    netlink.Link

	log          *logrus.Logger

}

type CfConfig struct {
	CellID                             string                `json:"cell_id,omitempty"`
	CellAddress                        string                `json:"cell_address,omitempty"`

	EtcdUrl                            string                `json:"etcd_url,omitempty"`
	EtcdCACertFile                     string                `json:"etcd_ca_cert_file"`
	EtcdClientCertFile                 string                `json:"etcd_client_cert_file"`
	EtcdClientKeyFile                  string                `json:"etcd_client_key_file"`

	CfNetOvsPort                       string                `json:"cf_net_ovs_port"`
	CfNetIntfAddress                   string                `json:"cf_net_interface_address"`
}

func NewCfEnvironment(config *HostAgentConfig, log *logrus.Logger) (*CfEnvironment, error) {
	if config.CfConfig == "" {
		err := errors.New("Path to CloudFoundry config file is empty")
		log.Error(err.Error())
		return nil, err
	}
	
	cfconfig := &CfConfig{}
	log.Info("Loading CF configuration from ", config.CfConfig)
	raw, err := ioutil.ReadFile(config.CfConfig)
	if err != nil {
		return nil, err
	}
	err = json.Unmarshal(raw, cfconfig)
	if err != nil {
		return nil, err
	}

	log.WithFields(logrus.Fields{
		"cfconfig":  config.CfConfig,
		"cell-id":   cfconfig.CellID,
	}).Info("Setting up CloudFoundry environment")

	etcdClient, err := etcd.NewEtcdClient(cfconfig.EtcdUrl, cfconfig.EtcdCACertFile,
										 cfconfig.EtcdClientCertFile, cfconfig.EtcdClientKeyFile)
	if err != nil {
		log.Error("Failed to create Etcd client: ", err)
		return nil, err
	}

	return &CfEnvironment{cellID: cfconfig.CellID, etcdClient: etcdClient, log: log,
						  cfconfig: cfconfig, indexLock: &sync.Mutex{}}, nil
}

func (env *CfEnvironment) Init(agent *HostAgent) error {
	env.agent = agent
	env.epIdx = make(map[string]etcd.EpInfo)
	env.ctPortMap = make(map[string]map[uint32]uint32)
	if env.cfconfig.CfNetOvsPort != "" {
		env.agent.ignoreOvsPorts[env.agent.config.IntBridgeName] = []string{env.cfconfig.CfNetOvsPort}
	}
	cellIp := net.ParseIP(env.cfconfig.CellAddress)
	if cellIp == nil {
		err := fmt.Errorf("Invalid cell IP address")
		return err
	}
	env.cfNetv4 = cellIp.To4() != nil
	var err error
	if env.cfNetv4 {
		env.iptbl, err = iptables.NewWithProtocol(iptables.ProtocolIPv4)
	} else {
		env.iptbl, err = iptables.NewWithProtocol(iptables.ProtocolIPv6)
	}
	return err
}

func (env *CfEnvironment) PrepareRun(stopCh <-chan struct{}) error {
	err := env.setupLegacyCfNet()
	if err != nil {
		env.log.Error("Error setting up legacy CF networking: ", err)
		return err
	}
	return env.initEtcdListener(stopCh)
}

func (env *CfEnvironment) CniDeviceChanged(metadataKey *string, id *md.ContainerId) {
	// TODO Find a better way to identify containers that we want to hide
	if strings.Contains(*metadataKey, "executor-healthcheck-") {
		return
	}

	ctId := id.Pod
	env.indexLock.Lock()
	ep, ok := env.epIdx[ctId]
	env.indexLock.Unlock()

	if !ok {
		env.log.Debug("No EP info for container ", ctId)
		return
	}
	env.cfAppChanged(&ctId, &ep)
}

func (env *CfEnvironment) CniDeviceDeleted(metadataKey *string, id *md.ContainerId) {
	env.cfAppDeleted(&id.Pod, nil)
}

func (env *CfEnvironment) cfAppChanged(ctId *string, ep *etcd.EpInfo) {
	if ep == nil {
		return
	}
	metaKey := "_cf_/" + *ctId

	epGroup := &md.OpflexGroup{ep.EpgTenant, ep.Epg}
	secGroup := make([]md.OpflexGroup, len(ep.SecurityGroups))
	for i, s := range ep.SecurityGroups {
		secGroup[i].PolicySpace = s.Tenant
		secGroup[i].Name = s.Group
	}

	epAttributes := make(map[string]string)
	if ep.AppName != "" {
		if ep.InstanceIndex < 0 {
			epAttributes["vm-name"] = ep.AppName + " (staging)"
		} else {
			epAttributes["vm-name"] = fmt.Sprintf("%s (%d)", ep.AppName, ep.InstanceIndex)
		}
	} else {
		epAttributes["vm-name"] = *ctId
	}
	epAttributes["app-id"] = ep.AppId
	epAttributes["container-id"] = *ctId

	env.agent.indexMutex.Lock()
	env.agent.epChanged(ctId, &metaKey, epGroup, secGroup, epAttributes, nil)
	env.updateLegacyCfNetService(ep.PortMapping)
	env.agent.indexMutex.Unlock()

	// Update iptables rules
	// pre-routing DNAT rules
	env.updatePreNatRule(ctId, ep, ep.PortMapping)
	// post-routing SNAT rules
	for _, pmap := range ep.PortMapping {
		cport := fmt.Sprintf("%d", pmap.ContainerPort)
		err := env.iptbl.AppendUnique("nat", NAT_POST_CHAIN, "-o", env.cfconfig.CfNetOvsPort, "-p", "tcp",
									 "-m", "tcp", "--dport", cport, "-j", "SNAT", "--to-source",
									 env.cfconfig.CfNetIntfAddress)
		if err != nil {
			env.log.Warning("Failed to add post-routing iptables rule: ", err)
		}
	}
}

func (env *CfEnvironment) updatePreNatRule(ctId *string, ep *etcd.EpInfo, portmap []etcd.PortMap) {
	ctIp := net.ParseIP(ep.IpAddress)
	if ctIp == nil || (env.cfNetv4 && ctIp.To4() == nil) {
		return
	}
	old_pm := env.ctPortMap[*ctId]
	new_pm := make(map[uint32]uint32)
	for _, ch := range portmap {
		err := env.iptbl.AppendUnique("nat", NAT_PRE_CHAIN, "-d", env.cfconfig.CellAddress, "-p", "tcp",
									 "--dport", fmt.Sprintf("%d", ch.HostPort),
									 "-j", "DNAT", "--to-destination",
									 ep.IpAddress + ":" + fmt.Sprintf("%d", ch.ContainerPort))
		if err != nil {
			env.log.Warning(fmt.Sprintf("Failed to add pre-routing iptables rule for %d: %v", *ctId, err))
		}
		new_pm[ch.HostPort] = ch.ContainerPort
		delete(old_pm, ch.HostPort)
	}
	for hp, cp := range old_pm {
		args := []string{"-d", env.cfconfig.CellAddress, "-p", "tcp", "--dport",
						 fmt.Sprintf("%d", hp), "-j", "DNAT", "--to-destination",
						 ep.IpAddress + ":" + fmt.Sprintf("%d", cp)}
		exist, _ := env.iptbl.Exists("nat", NAT_PRE_CHAIN, args...)
		if !exist {
			continue
		}
		err := env.iptbl.Delete("nat", NAT_PRE_CHAIN, args...)
		if err != nil {
			env.log.Warning(fmt.Sprintf("Failed to delete pre-routing iptables rule for %d: %v", *ctId, err))
		}
	}
	env.ctPortMap[*ctId] = new_pm
}

func (env *CfEnvironment) cfAppDeleted(ctId *string, ep *etcd.EpInfo) {
	env.agent.indexMutex.Lock()
	env.agent.epDeleted(ctId)
	env.agent.indexMutex.Unlock()

	if ep == nil {
		return
	}
	env.updatePreNatRule(ctId, ep, nil)
	delete(env.ctPortMap, *ctId)
}

func (env *CfEnvironment) setupLegacyCfNet() error {
	// set ip to interface that receives legacy CF networking traffic
	intfIp := net.ParseIP(env.cfconfig.CfNetIntfAddress)
	if intfIp == nil || (env.cfNetv4 && intfIp.To4() == nil) {
		err := fmt.Errorf("CF legacy network interface IP is not a valid IP address")
		return err
	}
	link, err := netlink.LinkByName(env.cfconfig.CfNetOvsPort)
	if err != nil {
		return err
	}
	linkAddr := netlink.Addr{IPNet: netlink.NewIPNet(intfIp)}
	linkAddr.Scope = syscall.IFA_LOCAL
	fam := netlink.FAMILY_V4
	if !env.cfNetv4 {
		fam = netlink.FAMILY_V6
	}
	allAddr, err := netlink.AddrList(link, fam)
	if err != nil {
		return err
	}
	addrFound := false
	for _, a := range allAddr {
		if a.Equal(linkAddr) {
			addrFound = true
			break
		}
	}
	if !addrFound {
		if err := netlink.AddrAdd(link, &linkAddr); err != nil {
			return err
		}
	}
	if err := netlink.LinkSetUp(link); err != nil {
		return err
	}

	// set routing rules to redirect traffic to the interface
	for _, n := range env.agent.config.NetConfig {
		dst := net.IPNet{IP: n.Subnet.IP, Mask: n.Subnet.Mask}
		route := netlink.Route{Dst: &dst, Gw: intfIp}
		if err := netlink.RouteReplace(&route); err != nil {
			return err
		}
	}

	// clear or create our iptables rule chains
	if err := env.iptbl.ClearChain("nat", NAT_PRE_CHAIN); err != nil {
		return err
	}
	if err := env.iptbl.ClearChain("nat", NAT_POST_CHAIN); err != nil {
		return err
	}
	// Link our chains from the pre/post-routing chains
	if err := env.iptbl.AppendUnique("nat", "PREROUTING", "-j", NAT_PRE_CHAIN); err != nil {
		return err
	}
	if err := env.iptbl.AppendUnique("nat", "POSTROUTING", "-j", NAT_POST_CHAIN); err != nil {
		return err
	}
	env.cfNetLink = link
	return nil
}

func (env *CfEnvironment) updateLegacyCfNetService(portmap []etcd.PortMap) error {
	// should be called with agent.indexMutex held
	uuid := "cf-net-" + env.cfconfig.CellID
	new_svc := opflexService{Uuid: uuid,
							DomainPolicySpace: env.agent.config.AciVrfTenant,
							DomainName: env.agent.config.AciVrf,
							ServiceMac: env.cfNetLink.Attrs().HardwareAddr.String(),
							InterfaceName: env.cfconfig.CfNetOvsPort}
	for _, pm := range portmap {
		svc_map := opflexServiceMapping{ServiceIp: env.cfconfig.CfNetIntfAddress,
									   ServicePort: uint16(pm.ContainerPort),
									   NextHopIps: make([]string, 0)}
		new_svc.ServiceMappings = append(new_svc.ServiceMappings, svc_map)
	}
	exist, ok := env.agent.opflexServices[uuid]
	if !ok || !reflect.DeepEqual(*exist, new_svc) {
		env.agent.opflexServices[uuid] = &new_svc
		env.agent.syncServices()
	}
	return nil
}
