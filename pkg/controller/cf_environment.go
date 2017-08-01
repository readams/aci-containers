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
	"encoding/json"
	"errors"
	"io/ioutil"
	"net/url"
	"sync"

	"code.cloudfoundry.org/bbs"
	"code.cloudfoundry.org/lager"

	etcdclient "github.com/coreos/etcd/client"
	"github.com/Sirupsen/logrus"

	"github.com/noironetworks/aci-containers/pkg/apicapi"
	"github.com/noironetworks/aci-containers/pkg/cfapi"
	"github.com/noironetworks/aci-containers/pkg/etcd"
)

type CfEnvironment struct {
	cont         *AciController
	cfconfig     *CfConfig

	bbsClient    bbs.Client
	ccClient     *cfapi.CcClient
	etcdClient   etcdclient.Client
	cfLogger     lager.Logger

	indexLock    sync.Locker
	contIdx      map[string]ContainerInfo
	appIdx       map[string]AppInfo
	spaceIdx     map[string]SpaceInfo
	asgIdx       map[string]SecurityGroupInfo

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

	AppPort                            string                `json:"app_port"`
	SshPort                            string                `json:"ssh_port"`
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
	

	ccClient, err := cfapi.NewCcClient(cfconfig.CCApiUrl, cfconfig.CCApiUsername,
									  cfconfig.CCApiPassword, log)
	if err != nil {
		log.Error("Failed to create CC API client: ", err)
		return nil, err
	}

	etcdClient, err := etcd.NewEtcdClient(cfconfig.EtcdUrl, cfconfig.EtcdCACertFile,
										 cfconfig.EtcdClientCertFile, cfconfig.EtcdClientKeyFile)
	if err != nil {
		log.Error("Failed to create Etcd client: ", err)
		return nil, err
	}

	return &CfEnvironment{cfconfig: cfconfig, bbsClient: bbsClient, ccClient: ccClient,
						 etcdClient: etcdClient, indexLock: &sync.Mutex{}, log: log}, nil
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

	return nil
}

func (env *CfEnvironment) PrepareRun(stopCh <-chan struct{}) error {
	appCh := make(chan AppUpdateInfo, 100)
	go env.initBbsEventListener(appCh, stopCh)
	go env.initBbsTaskListener(appCh, stopCh)
	go env.initAppIndexBuilder(appCh, stopCh)
	return nil
}

func (env *CfEnvironment) InitStaticAciObjects() {
	env.initStaticHpp()
}

func (env *CfEnvironment) initStaticHpp() {
	cont := env.cont

	staticName := cont.aciNameForKey("asg", "static")
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

	cont.apicConn.WriteApicObjects(cont.config.AciPrefix + "_asg_static", apicapi.ApicSlice{hpp})
}
