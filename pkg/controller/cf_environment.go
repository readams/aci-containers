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

package controller

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"net/url"
	"sync"
	"time"

	"code.cloudfoundry.org/bbs"
	"code.cloudfoundry.org/lager"

	etcdclient "github.com/coreos/etcd/client"
	"github.com/Sirupsen/logrus"
	"golang.org/x/net/context"

	"github.com/noironetworks/aci-containers/pkg/apicapi"
	"github.com/noironetworks/aci-containers/pkg/cfapi"
	"github.com/noironetworks/aci-containers/pkg/etcd"
	"github.com/noironetworks/aci-containers/pkg/ipam"
)

type CfEnvironment struct {
	cont         *AciController
	cfconfig     *CfConfig

	bbsClient    bbs.Client
	ccClient     cfapi.CcClient
	etcdKeysApi  etcdclient.KeysAPI
	netpolClient *cfapi.PolicyClient
	cfAuthClient cfapi.CfAuthClient
	cfLogger     lager.Logger
	db           *sql.DB

	indexLock    sync.Locker
	contIdx      map[string]ContainerInfo
	appIdx       map[string]AppInfo
	spaceIdx     map[string]SpaceInfo
	asgIdx       map[string]SecurityGroupInfo
	netpolIdx    map[string]map[string][]cfapi.Destination
	isoSegIdx    map[string]IsoSegInfo

	appVips      *netIps

	idxStatusChan   chan string
	log          *logrus.Logger
}

type CfConfig struct {
	BBSAddress                         string                `json:"bbs_address"`
	BBSCACertFile                      string                `json:"bbs_ca_cert_file"`
	BBSClientCertFile                  string                `json:"bbs_client_cert_file"`
	BBSClientKeyFile                   string                `json:"bbs_client_key_file"`
	BBSClientSessionCacheSize          int                   `json:"bbs_client_session_cache_size,omitempty"`
	BBSMaxIdleConnsPerHost             int                   `json:"bbs_max_idle_conns_per_host,omitempty"`

	CCApiUrl                           string                `json:"cc_api_url,omitempty"`
	CCApiUsername                      string                `json:"cc_api_username,omitempty"`
	CCApiPassword                      string                `json:"cc_api_password,omitempty"`

	EtcdUrl                            string                `json:"etcd_url,omitempty"`
	EtcdCACertFile                     string                `json:"etcd_ca_cert_file"`
	EtcdClientCertFile                 string                `json:"etcd_client_cert_file"`
	EtcdClientKeyFile                  string                `json:"etcd_client_key_file"`

	UaaUrl                             string                `json:"uaa_url,omitempty"`
	UaaCACertFile                      string                `json:"uaa_ca_cert_file"`
	UaaClientName                      string                `json:"uaa_client_name"`
	UaaClientSecret                    string                `json:"uaa_client_secret"`

	NetPolApiUrl                       string                `json:"network_policy_api_url"`
	NetPolCACertFile                   string                `json:"network_policy_ca_cert_file"`
	NetPolClientCertFile               string                `json:"network_policy_client_cert_file"`
	NetPolClientKeyFile                string                `json:"network_policy_client_key_file"`
	NetPolPollingInterval              int                   `json:"network_policy_polling_interval_sec"`

	DbType                             string                `json:"db_type"`
	DbDsn                              string                `json:"db_dsn"`

	ApiPathPrefix                      string                `json:"api_path_prefix"`

	AppPort                            string                `json:"app_port"`
	SshPort                            string                `json:"ssh_port"`

	DefaultAppProfile                  string                `json:"default_app_profile"`

	// Virtual IP address pool for apps
	AppVipPool                         []ipam.IpRange        `json:"app_vip_pool,omitempty"`
	AppVipSubnet                       []string              `json:"app_vip_subnet,omitempty"`
}

func NewCfEnvironment(config *ControllerConfig, log *logrus.Logger) (*CfEnvironment, error) {
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
	if cfconfig.AppPort == "" {
		cfconfig.AppPort = "8080"
	}
	if cfconfig.SshPort == "" {
		cfconfig.SshPort = "2222"
	}
	if cfconfig.NetPolPollingInterval <= 0 {
		cfconfig.NetPolPollingInterval = 1
	}
	if cfconfig.ApiPathPrefix == "" {
		cfconfig.ApiPathPrefix = "/networking-aci"
	}

	log.WithFields(logrus.Fields{
		"cfconfig":  config.CfConfig,
	}).Info("Setting up CloudFoundry environment")

	log.Debug("Initializing BBS client")
	bbsURL, err := url.Parse(cfconfig.BBSAddress)
	if err != nil {
		log.Error("Invalid BBS URL: ", err)
		return nil, err
	}
	
	var bbsClient bbs.Client
	if bbsURL.Scheme != "https" {
		bbsClient = bbs.NewClient(cfconfig.BBSAddress)
	} else {
		bbsClient, err = bbs.NewSecureClient(
			cfconfig.BBSAddress,
			cfconfig.BBSCACertFile,
			cfconfig.BBSClientCertFile,
			cfconfig.BBSClientKeyFile,
			cfconfig.BBSClientSessionCacheSize,
			cfconfig.BBSMaxIdleConnsPerHost,
		)
		if err != nil {
			log.Error("Failed to configure secure BBS client: ", err)
			return nil, err
		}
	}

	etcdClient, err := etcd.NewEtcdClient(cfconfig.EtcdUrl, cfconfig.EtcdCACertFile,
										 cfconfig.EtcdClientCertFile, cfconfig.EtcdClientKeyFile)
	if err != nil {
		log.Error("Failed to create Etcd client: ", err)
		return nil, err
	}
	etcdKeysApi := etcdclient.NewKeysAPI(etcdClient)

	netpolClient, err := cfapi.NewNetPolClient(cfconfig.NetPolApiUrl,
											  cfconfig.NetPolCACertFile,
											  cfconfig.NetPolClientCertFile,
											  cfconfig.NetPolClientKeyFile)
	if err != nil {
		log.Error("Failed to create network policy client: ", err)
		return nil, err
	}

	authClient, err := cfapi.NewCfAuthClient(cfconfig.UaaUrl, cfconfig.UaaCACertFile,
											cfconfig.UaaClientName, cfconfig.UaaClientSecret)
	if err != nil {
		log.Error("Failed to create UAA client: ", err)
		return nil, err
	}

	return &CfEnvironment{cfconfig: cfconfig, bbsClient: bbsClient,
						 etcdKeysApi: etcdKeysApi, netpolClient: netpolClient,
						 cfAuthClient: authClient,
						 indexLock: &sync.Mutex{}, 
						 appVips: newNetIps(),
						 log: log}, nil
}

func (env *CfEnvironment) VmmPolicy() string {
	return "CloudFoundry"
}

func (env *CfEnvironment) ServiceBd() string {
	return "cf-app-ext-ip"
}

func (env *CfEnvironment) Init(cont *AciController) error {
	env.cont = cont
	
	env.cfLogger = lager.NewLogger("CfEnv")
	lagerLevel := lager.INFO
	switch env.log.Level {
	case logrus.DebugLevel:
		lagerLevel = lager.DEBUG
	case logrus.InfoLevel:
		lagerLevel = lager.INFO
	case logrus.WarnLevel:
		lagerLevel = lager.INFO
	case logrus.ErrorLevel:
		lagerLevel = lager.ERROR
	case logrus.FatalLevel:
		lagerLevel = lager.FATAL
	case logrus.PanicLevel:
		lagerLevel = lager.FATAL
	default:
	}
	env.cfLogger.RegisterSink(lager.NewWriterSink(env.log.Out, lagerLevel))

	env.contIdx = make(map[string]ContainerInfo)
	env.appIdx = make(map[string]AppInfo)
	env.spaceIdx = make(map[string]SpaceInfo)
	env.asgIdx = make(map[string]SecurityGroupInfo)
	env.netpolIdx = make(map[string]map[string][]cfapi.Destination)
	env.isoSegIdx = make(map[string]IsoSegInfo)

	cont.loadIpRanges(env.appVips.V4, env.appVips.V6, env.cfconfig.AppVipPool)

	var err error
	env.db, err = sql.Open(env.cfconfig.DbType, env.cfconfig.DbDsn)
	if err != nil {
		env.log.Error("Unable to open SQL DB: ", err)
		return err
	}

	env.idxStatusChan = make(chan string)
	return nil
}

func (env *CfEnvironment) PrepareRun(stopCh <-chan struct{}) error {
	var err error

	// test DB connectivity
	if err = env.db.Ping(); err != nil {
		env.log.Error("Unable to connect to SQL DB: ", err)
		return err
	}
	if err = env.RunDbMigration(); err != nil {
		env.log.Error("Failed to run DB migration: ", err)
		return err
	}

	epg_anno_handler := EpgAnnotationHttpHandler{env: env}
	app_vip_handler := AppVipHttpHandler{env: env}
	http.Handle(epg_anno_handler.Path(), &epg_anno_handler)
	http.Handle(app_vip_handler.Path(), &app_vip_handler)

	maxattempts := 240                 // TODO move the connect loop to app-index builder
	for env.ccClient == nil && maxattempts > 0 {
		maxattempts--
		ccClient, err := cfapi.NewCcClient(env.cfconfig.CCApiUrl, env.cfconfig.CCApiUsername,
										  env.cfconfig.CCApiPassword)
		if err != nil {
			env.log.Error("Failed to create CC API client: ", err)
			time.Sleep(5 * time.Second)
			continue
		}
		env.log.Debug("CC API client created")
		env.ccClient = ccClient
	}
	if env.ccClient == nil {
		env.log.Error("Couldn't create CC API client, aborting: ", err)
		return err
	}

	env.LoadAppExtIps()

	appCh := make(chan interface{}, 100)
	go env.initBbsEventListener(appCh, stopCh)
	go env.initBbsTaskListener(appCh, stopCh)
	go env.initAppIndexBuilder(appCh, stopCh)
	go env.initNetworkPolicyPoller(stopCh)
	env.log.Info("Waiting for initial sync")
	var idx_ready, net_pol_ready bool = false, false
	for !idx_ready || !net_pol_ready {
		select {
		case <-stopCh:
			return nil

		case status := <-env.idxStatusChan:
			idx_ready = idx_ready || (status == "index-ready")
			net_pol_ready = net_pol_ready || (status == "net-policy-ready")
			env.log.Debug(
				fmt.Sprintf("Sync wait: index-ready %v, net-pol-ready %v", idx_ready, net_pol_ready))
		}
	}
	env.log.Info("Initial sync complete")
	return nil
}

func (env *CfEnvironment) InitStaticAciObjects() {
	env.initStaticHpp()
	env.cont.initStaticServiceObjs()
}

func (env *CfEnvironment) initStaticHpp() {
	cont := env.cont

	staticName := cont.aciNameForKey("hpp", "static")
	hpp := apicapi.NewHostprotPol(cont.config.AciPolicyTenant, staticName)

	// ARP ingress/egress and ICMP ingress (+ reply)
	discSubj := apicapi.NewHostprotSubj(hpp.GetDn(), "discovery")
	discDn := discSubj.GetDn()
	{
		arpin := apicapi.NewHostprotRule(discDn, "arp-ingress")
		arpin.SetAttr("direction", "ingress")
		arpin.SetAttr("ethertype", "arp")
		arpin.SetAttr("connTrack", "normal")
		discSubj.AddChild(arpin)
	}
	{
		arpout := apicapi.NewHostprotRule(discDn, "arp-egress")
		arpout.SetAttr("direction", "egress")
		arpout.SetAttr("ethertype", "arp")
		arpout.SetAttr("connTrack", "normal")
		discSubj.AddChild(arpout)
	}
	{
		icmpin := apicapi.NewHostprotRule(discDn, "icmp-ingress")
		icmpin.SetAttr("direction", "ingress")
		icmpin.SetAttr("ethertype", "ipv4")
		icmpin.SetAttr("protocol", "icmp")
		discSubj.AddChild(icmpin)
	}
	hpp.AddChild(discSubj)

	// Default app port and SSH port - ingress (+reply) allowed
	appSubj := apicapi.NewHostprotSubj(hpp.GetDn(), "app")
	appDn := appSubj.GetDn()
	{
		appPort := apicapi.NewHostprotRule(appDn, "app-port")
		appPort.SetAttr("direction", "ingress")
		appPort.SetAttr("ethertype", "ipv4")
		appPort.SetAttr("toPort", env.cfconfig.AppPort)
		appPort.SetAttr("protocol", "tcp")
		appSubj.AddChild(appPort)

		appSsh := apicapi.NewHostprotRule(appDn, "app-ssh")
		appSsh.SetAttr("direction", "ingress")
		appSsh.SetAttr("ethertype", "ipv4")
		appSsh.SetAttr("toPort", env.cfconfig.SshPort)
		appSsh.SetAttr("protocol", "tcp")
		appSubj.AddChild(appSsh)
	}
	hpp.AddChild(appSubj)

	// Allow TCP/UDP egress (+ reply) to CNI network and app-VIP network
	subnets := make([]string, 0)
	if env.cont.config.PodSubnet != "" {
		subnets = append(subnets, env.cont.config.PodSubnet)
	} else {
		env.log.Warning("Pod subnet not defined")
	}
	for _, vip_sn := range env.cfconfig.AppVipSubnet {
		subnets = append(subnets, vip_sn)
	}
	for idx, subnet_str := range subnets {
		_, subnet, err := net.ParseCIDR(subnet_str)
		if err != nil {
			env.log.Warning(fmt.Sprintf("Invalid subnet %s: ", subnet_str), err)
		} else {
			af := "ipv4"
			if subnet.IP.To4() == nil && subnet.IP.To16() != nil {
				af = "ipv6"
			}
			c2cSubj := apicapi.NewHostprotSubj(hpp.GetDn(), fmt.Sprintf("c2c-%d", idx))
			c2cDn := c2cSubj.GetDn()
			tcp := apicapi.NewHostprotRule(c2cDn, "c2c-tcp")
			tcp.SetAttr("direction", "egress")
			tcp.SetAttr("ethertype", af)
			tcp.SetAttr("protocol", "tcp")
			tcp_remote := apicapi.NewHostprotRemoteIp(tcp.GetDn(), subnet.String())
			tcp.AddChild(tcp_remote)
			c2cSubj.AddChild(tcp)

			udp := apicapi.NewHostprotRule(c2cDn, "c2c-udp")
			udp.SetAttr("direction", "egress")
			udp.SetAttr("ethertype", af)
			udp.SetAttr("protocol", "udp")
			udp_remote := apicapi.NewHostprotRemoteIp(udp.GetDn(), subnet.String())
			udp.AddChild(udp_remote)
			c2cSubj.AddChild(udp)

			hpp.AddChild(c2cSubj)
		}
	}
	cont.apicConn.WriteApicObjects(cont.config.AciPrefix + "_asg_static", apicapi.ApicSlice{hpp})
}

// must be called with cont.indexMutex locked
func (env *CfEnvironment) LoadCellNetworkInfo(cellId string) {
	if _, ok := env.cont.nodePodNetCache[cellId]; ok {
		return
	}
	kapi := env.etcdKeysApi
	cellKey := etcd.CELL_KEY_BASE + "/" + cellId + "/network"
	resp, err := kapi.Get(context.Background(), cellKey, nil)
	if err != nil {
		keyerr, ok := err.(etcdclient.Error)
		if ok && keyerr.Code == etcdclient.ErrorCodeKeyNotFound {
			env.log.Info(fmt.Sprintf("Etcd subtree %s doesn't exist yet", cellKey))
		} else {
			env.log.Error("Unable to fetch etcd cell network info: ", err)
		}
		return
	}
	nodePodNet := newNodePodNetMeta()
	env.cont.nodePodNetCache[cellId] = nodePodNet
	env.cont.mergePodNet(nodePodNet, resp.Node.Value, logrus.NewEntry(env.log))
}

// must be called with cont.indexMutex locked
func (env *CfEnvironment) LoadCellServiceInfo(cellId string) bool {
	nodeName := "diego-cell-" + cellId
	if _, ok := env.cont.nodeServiceMetaCache[nodeName]; ok {
		return false
	}
	nodeMeta := &nodeServiceMeta{}
	kapi := env.etcdKeysApi
	cellKey := etcd.CELL_KEY_BASE + "/" + cellId + "/service"
	resp, err := kapi.Get(context.Background(), cellKey, nil)
	if err != nil {
		keyerr, ok := err.(etcdclient.Error)
		if ok && keyerr.Code == etcdclient.ErrorCodeKeyNotFound {
			env.log.Info(fmt.Sprintf("Etcd subtree %s doesn't exist yet", cellKey))
		} else {
			env.log.Error("Unable to fetch etcd cell service info: ", err)
			return false
		}
	} else {
		err = json.Unmarshal([]byte(resp.Node.Value), &nodeMeta.serviceEp)
		if err != nil {
			env.log.Warn("Could not parse cell service info: ", err)
		}
	}
	err = env.cont.createServiceEndpoint(&nodeMeta.serviceEp)
	if err != nil {
		env.log.Error("Couldn't create service EP info for cell: ", err)
		return false
	}
	raw, err := json.Marshal(&nodeMeta.serviceEp)
	if err != nil {
		env.log.Error("Could not marshal cell service info: ", err)
	} else {
		_, err = kapi.Set(context.Background(), cellKey, string(raw), nil)
		if err != nil {
			env.log.Error("Error setting etcd service info for cell: ", err)
		}
	}
	if err == nil {
		env.cont.nodeServiceMetaCache[nodeName] = nodeMeta
		return true
	} else {
		if nodeMeta.serviceEp.Ipv4 != nil {
			env.cont.nodeServiceIps.V4.AddIp(nodeMeta.serviceEp.Ipv4)
		}
		if nodeMeta.serviceEp.Ipv6 != nil {
			env.cont.nodeServiceIps.V6.AddIp(nodeMeta.serviceEp.Ipv6)
		}
	}
	return false
}

// must be called with cont.indexMutex locked
func (env *CfEnvironment) NodePodNetworkChanged(nodename string) {
	env.cont.indexMutex.Lock()
	podnet, ok := env.cont.nodePodNetCache[nodename]
	env.cont.indexMutex.Unlock()
	if ok {
		cellKey := etcd.CELL_KEY_BASE + "/" + nodename
		kapi := env.etcdKeysApi
		_, err := kapi.Set(context.Background(), cellKey + "/network", podnet.podNetIpsAnnotation, nil)
		if err != nil {
			env.log.Error("Error setting etcd net info for cell: ", err)
		}
	}
}

func (env *CfEnvironment) NodeServiceChanged(nodeName string) {
}
