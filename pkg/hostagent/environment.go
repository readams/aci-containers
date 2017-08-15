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
	"os"
	"strings"
	"sync"

	etcdclient "github.com/coreos/etcd/client"
	"github.com/Sirupsen/logrus"

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

type CfEnvironment struct {
	cellID       string
	agent        *HostAgent
	etcdClient   etcdclient.Client

	indexLock    sync.Locker
	epIdx        map[string]etcd.EpInfo

	log          *logrus.Logger
}

type CfConfig struct {
	CellID                             string                `json:"cell_id,omitempty"`

	EtcdUrl                            string                `json:"etcd_url,omitempty"`
	EtcdCACertFile                     string                `json:"etcd_ca_cert_file"`
	EtcdClientCertFile                 string                `json:"etcd_client_cert_file"`
	EtcdClientKeyFile                  string                `json:"etcd_client_key_file"`
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
						  indexLock: &sync.Mutex{}}, nil
}

func (env *CfEnvironment) Init(agent *HostAgent) error {
	env.agent = agent
	env.epIdx = make(map[string]etcd.EpInfo)
	return nil
}

func (env *CfEnvironment) PrepareRun(stopCh <-chan struct{}) error {
	return env.initEtcdListener(stopCh)
}

func (env *CfEnvironment) CniDeviceChanged(metadataKey *string, id *md.ContainerId) {
	env.cfAppChanged(&id.Pod, metadataKey)
}

func (env *CfEnvironment) CniDeviceDeleted(metadataKey *string, id *md.ContainerId) {
	env.cfAppDeleted(&id.Pod)
}

func (env *CfEnvironment) cfAppChanged(ctId *string, metaKey *string) {
	// TODO Find a better way to identify containers that we want to hide
	if strings.Contains(*metaKey, "executor-healthcheck-") {
		return
	}

	env.indexLock.Lock()
	ep, ok := env.epIdx[*ctId]
	env.indexLock.Unlock()
	if !ok {
		env.log.Debug("No EP info for container ", *ctId)
		return
	}

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
	defer env.agent.indexMutex.Unlock()
	env.agent.epChanged(ctId, metaKey, epGroup, secGroup, epAttributes, nil)
}

func (env *CfEnvironment) cfAppDeleted(ctId *string) {
	env.agent.indexMutex.Lock()
	defer env.agent.indexMutex.Unlock()
	env.agent.epDeleted(ctId)
}
