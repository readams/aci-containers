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
	"sync"

	"github.com/Sirupsen/logrus"

//	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"

	"github.com/noironetworks/aci-containers/pkg/ipam"
	md "github.com/noironetworks/aci-containers/pkg/metadata"
)

type HostAgent struct {
	log    *logrus.Logger
	config *HostAgentConfig
	env    Environment

	indexMutex sync.Mutex

	opflexEps      map[string][]*opflexEndpoint
	opflexServices map[string]*opflexService
	epMetadata     map[string]map[string]*md.ContainerMetadata
	serviceEp      md.ServiceEndpoint

	podInformer       cache.SharedIndexInformer
	endpointsInformer cache.SharedIndexInformer
	serviceInformer   cache.SharedIndexInformer
	nodeInformer      cache.SharedIndexInformer

	podNetAnnotation string
	podIpsV4         []*ipam.IpAlloc
	podIpsV6         []*ipam.IpAlloc

	syncEnabled         bool
	opflexConfigWritten bool

	ignoreOvsPorts      map[string][]string

	netNsFuncChan chan func()
}

func NewHostAgent(config *HostAgentConfig, env Environment, log *logrus.Logger) *HostAgent {
	return &HostAgent{
		log:            log,
		config:         config,
		env:            env,
		opflexEps:      make(map[string][]*opflexEndpoint),
		opflexServices: make(map[string]*opflexService),
		epMetadata:     make(map[string]map[string]*md.ContainerMetadata),

		podIpsV4: []*ipam.IpAlloc{ipam.New(), ipam.New()},
		podIpsV6: []*ipam.IpAlloc{ipam.New(), ipam.New()},

		ignoreOvsPorts: make(map[string][]string),

		netNsFuncChan: make(chan func()),
	}
}

func (agent *HostAgent) Init() {
	agent.log.Debug("Initializing endpoint CNI metadata")
	err := md.LoadMetadata(agent.config.CniMetadataDir,
		agent.config.CniNetwork, &agent.epMetadata)
	if err != nil {
		panic(err.Error())
	}
	agent.log.Info("Loaded cached endpoint CNI metadata: ", len(agent.epMetadata))

	err = agent.env.Init(agent)
	if err != nil {
		panic(err.Error())
	}
}

func (agent *HostAgent) Run(stopCh <-chan struct{}) {
	err := agent.env.PrepareRun(stopCh)
	if err != nil {
		panic(err.Error())
	}

//    if agent.config.PodNetworkRanges != "" {
//	    	agent.updateIpamAnnotation(agent.config.PodNetworkRanges)
//    }
//	agent.log.Debug("Building IP address management database")
//	agent.rebuildIpam()

	if agent.config.OpFlexEndpointDir == "" ||
		agent.config.OpFlexServiceDir == "" {
		agent.log.Warn("OpFlex endpoint and service directories not set")
	} else {
		agent.log.Info("Enabling OpFlex endpoint and service sync")
		agent.indexMutex.Lock()
		agent.syncEnabled = true
		agent.syncServices()
		agent.syncEps()
		agent.indexMutex.Unlock()
		agent.log.Debug("Initial OpFlex sync complete")
	}

	agent.log.Info("Starting endpoint RPC")
	err = agent.runEpRPC(stopCh)
	if err != nil {
		panic(err.Error())
	}

	agent.cleanupSetup()
}
