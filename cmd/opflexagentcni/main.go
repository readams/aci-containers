// Copyright 2014 CNI authors
// Copyright 2016 Cisco Systems, Inc.
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

package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"runtime"
	"time"

	"code.cloudfoundry.org/garden"

	"github.com/Sirupsen/logrus"
	"github.com/containernetworking/cni/pkg/ipam"
	"github.com/containernetworking/cni/pkg/ns"
	"github.com/containernetworking/cni/pkg/skel"
	"github.com/containernetworking/cni/pkg/types"
	"github.com/containernetworking/cni/pkg/types/current"
	"github.com/containernetworking/cni/pkg/version"
	"github.com/tatsushid/go-fastping"

	"github.com/noironetworks/aci-containers/pkg/eprpcclient"
	cnimd "github.com/noironetworks/aci-containers/pkg/metadata"
)

var log = logrus.New()
var logFile string
var logFd os.File

func init() {
	// This ensures that main runs only on main thread (thread group leader).
	// since namespace ops (unshare, setns) are done for a single thread, we
	// must ensure that the goroutine does not jump from OS thread to thread
	runtime.LockOSThread()
}

type K8SArgs struct {
	K8S_POD_NAME               types.UnmarshallableString
	K8S_POD_NAMESPACE          types.UnmarshallableString
	K8S_POD_INFRA_CONTAINER_ID types.UnmarshallableString
}

type CfMetadata struct {
	PolicyGroupId string `json:"policy_group_id,omitempty"`
	AppId string `json:"app_id,omitempty"`
	SpaceId string `json:"space_id,omitempty"`
	OrgId string `json:"org_id,omitempty"`
}

type CfRuntimeConfig struct {
	PortMappings []garden.NetIn      `json:"portMappings"`
	NetOutRules  []garden.NetOutRule `json:"netOutRules"`
}

type NetConf struct {
	types.NetConf
	RuntimeConfig  CfRuntimeConfig `json:"runtimeConfig,omitempty"`
	CfMetadata     CfMetadata      `json:"metadata,omitempty"`
	LogLevel       string `json:"log-level,omitempty"`
	LogFile        string `json:"log-file,omitempty"`
	WaitForNetwork bool   `json:"wait-for-network"`
	EpRpcSock      string `json:"ep-rpc-sock,omitempty"`
}

func loadConf(args *skel.CmdArgs) (*NetConf, *K8SArgs, string, error) {
	n := &NetConf{
		EpRpcSock: "/var/run/aci-containers-ep-rpc.sock",
	}
	if err := json.Unmarshal(args.StdinData, n); err != nil {
		return nil, nil, "", fmt.Errorf("failed to load netconf: %v", err)
	}

	log.Out = os.Stderr
	if n.LogFile != "" {
		logFd, err := os.OpenFile(n.LogFile, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0666)
		if err == nil {
			log.Out = logFd
			logFile = n.LogFile
		} else {
			log.Info("Failed to open log file, using stderr: ", err)
		}
	}

	logLevel, err := logrus.ParseLevel(n.LogLevel)
	if err == nil {
		log.Level = logLevel
	}

	log.Debug("NetConf ", n)
	stdinStr := string(args.StdinData)
	log.Debug("StdinData ", stdinStr)

	k8sArgs := &K8SArgs{}
	err = types.LoadArgs(args.Args, k8sArgs)
	if err != nil {
		return nil, nil, "", err
	}
	
	if k8sArgs.K8S_POD_NAME == "" {
		k8sArgs.K8S_POD_NAMESPACE = "_cf_"
		k8sArgs.K8S_POD_NAME = types.UnmarshallableString(args.ContainerID)
	}

	id := args.ContainerID
	log.Debug("k8sArgs ", k8sArgs)
	log.Debug("ContainerID ", id)

	return n, k8sArgs, id, nil
}

func waitForAllNetwork(result *current.Result, id string,
	timeout time.Duration) {

	for index, iface := range result.Interfaces {
		netns, err := ns.GetNS(iface.Sandbox)
		if err != nil {
			log.Error("Could not open netns: ", err)
		} else {
			waitForNetwork(netns, result, id, index, 10*time.Second)
			netns.Close()
		}
	}

}

func waitForNetwork(netns ns.NetNS, result *current.Result,
	id string, index int, timeout time.Duration) {

	logger := log.WithFields(logrus.Fields{
		"id": id,
	})

	end := time.Now().Add(timeout)
	now := time.Now()
	for now.Before(end) {
		if err := netns.Do(func(hostNS ns.NetNS) error {
			pinger := fastping.NewPinger()
			pinger.MaxRTT = time.Millisecond * 100
			expected := 0
			for _, ip := range result.IPs {
				if ip.Gateway == nil || ip.Interface != index {
					continue
				}
				pinger.AddIPAddr(&net.IPAddr{IP: ip.Gateway})
				expected += 1
			}
			if expected == 0 {
				logger.Debug("Network configuration has no gateway")
				return nil
			}

			count := 0
			pinger.OnRecv = func(addr *net.IPAddr, rtt time.Duration) {
				logger.WithFields(logrus.Fields{
					"IP":  addr,
					"rtt": rtt,
				}).Debug("Received")
				count += 1
			}

			err := pinger.Run()
			if err != nil {
				return err
			}
			if count >= expected {
				return nil
			}
			return errors.New("Ping failed")
		}); err == nil {
			return
		}

		now = time.Now()
	}

	logger.Error("Gave up waiting for network")
}

func cmdAdd(args *skel.CmdArgs) error {
	n, k8sArgs, id, err := loadConf(args)
	if err != nil {
		return err
	}

	logger := log.WithFields(logrus.Fields{
		"id": id,
	})
	log.Info("CMD ADD")

	// run the IPAM plugin and get back the config to apply
	var result *current.Result
	if n.IPAM.Type != "opflex-agent-cni-ipam" {
		logger.Debug("Executing IPAM add")
		r, err := ipam.ExecAdd(n.IPAM.Type, args.StdinData)
		if err != nil {
			return err
		}
		result, err = current.NewResultFromResult(r)
		if err != nil {
			return err
		}
		if len(result.IPs) == 0 {
			return errors.New("IPAM plugin returned missing IP config")
		}
		for _, ip := range result.IPs {
			ip.Interface = 0
		}
	} else {
		result = &current.Result{}
		result.DNS = n.DNS
	} 

	metadata := cnimd.ContainerMetadata{
		Id: cnimd.ContainerId{
			ContId:    id,
			Namespace: string(k8sArgs.K8S_POD_NAMESPACE),
			Pod:       string(k8sArgs.K8S_POD_NAME),
		},
		Ifaces: []*cnimd.ContainerIfaceMd{
			&cnimd.ContainerIfaceMd{
				Name:    args.IfName,
				Sandbox: args.Netns,
			},
		},
	}

	logger.Debug("Registering with host agent")

	eprpc, err := eprpcclient.NewClient(n.EpRpcSock, time.Millisecond*500)
	if err != nil {
		return err
	}

	result, err = eprpc.Register(&metadata)
	if err != nil {
		return err
	}

	if n.WaitForNetwork {
		logger.Debug("Waiting for network connectivity")
		waitForAllNetwork(result, id, 10*time.Second)
	}

	logger.Debug("ADD result: ", result)
	return result.Print()
}

func cmdDel(args *skel.CmdArgs) error {
	n, k8sArgs, id, err := loadConf(args)
	if err != nil {
		return err
	}
	logger := log.WithFields(logrus.Fields{
		"id": id,
	})
	log.Info("CMD DEL")


	if n.IPAM.Type != "opflex-agent-cni-ipam" {
		logger.Debug("Executing IPAM delete")
		if err := ipam.ExecDel(n.IPAM.Type, args.StdinData); err != nil {
			return err
		}
	}

	cid := &cnimd.ContainerId{
		ContId:    id,
		Pod:       string(k8sArgs.K8S_POD_NAME),
		Namespace: string(k8sArgs.K8S_POD_NAMESPACE),
	}

	logger.Debug("Unregistering with host agent")

	eprpc, err := eprpcclient.NewClient(n.EpRpcSock, time.Millisecond*500)
	if err != nil {
		return err
	}
	_, err = eprpc.Unregister(cid)
	if err != nil {
		return err
	}

	return nil
}

func main() {
	skel.PluginMain(cmdAdd, cmdDel,
		version.PluginSupports("0.3.0", "0.3.1"))
	
	if logFile != "" {
	    logFd.Close()
	}
}
