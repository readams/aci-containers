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
	"net"
	"strings"
	
	"golang.org/x/net/context"
	etcdclient "github.com/coreos/etcd/client"
	
	"github.com/noironetworks/aci-containers/pkg/etcd"
	"github.com/noironetworks/aci-containers/pkg/ipam"
	"github.com/noironetworks/aci-containers/pkg/metadata"
)

func isDeleteAction(action *string) bool {
	return (*action == "delete" || *action == "compareAndDelete" || *action == "expire")
}

func (env *CfEnvironment) getDefaultIpPool() (string) {
	ipa4 := ipam.New()
	ipa6 := ipam.New()

	for _, nc := range env.agent.config.NetConfig {
		ip := nc.Subnet.IP.To4()
		num_ones, _ := nc.Subnet.Mask.Size()
		mask := net.CIDRMask(num_ones, 32)
		gw := nc.Gateway.To4()
		ipa := ipa4
		if ip == nil {
			ip = nc.Subnet.IP.To16()
			mask = net.CIDRMask(num_ones, 128)
			gw = nc.Gateway.To16()
			ipa = ipa6
			if ip == nil {
				continue
			}
		}
		last := make(net.IP, len(ip))
		for i := 0; i < len(ip); i++ {
			last[i] = ip[i] | ^mask[i]
		}
		// TODO add a random offset to start address
		ipa.AddRange(ip, last)
		// remove the start address and gateway address
		ipa.RemoveIp(ip)
		ipa.RemoveIp(gw)
	}

	netips := metadata.NetIps{V4: ipa4.FreeList, V6: ipa6.FreeList}
	raw, err := json.Marshal(&netips)
	if err != nil {
		env.log.Error("Could not create default ip-pool", err)
		return ""
	}
	env.log.Debug("Setting default IP pool to ", string(raw))
	return string(raw)
}

func (env *CfEnvironment) initEtcdListener(stopCh <-chan struct{}) error {
	kapi := etcdclient.NewKeysAPI(env.etcdClient)
	cellKey := etcd.CELL_KEY_BASE + "/" + env.cellID
	cellNetKey := cellKey + "/network"
	ctBaseKey := cellKey + "/containers/"
	w := kapi.Watcher(cellKey, &etcdclient.WatcherOptions{Recursive: true})
	app_w := kapi.Watcher(etcd.APP_KEY_BASE,
						  &etcdclient.WatcherOptions{Recursive: true})
	cellNetNodeExists := false

	var nodes etcdclient.Nodes
	keys_to_watch := []string{cellKey, etcd.APP_KEY_BASE}
	for _, key := range keys_to_watch {
		resp, err := kapi.Get(context.Background(), key,
							  &etcdclient.GetOptions{Recursive: true})
		if err != nil {
			keyerr, ok := err.(etcdclient.Error)
			if ok && keyerr.Code == etcdclient.ErrorCodeKeyNotFound {
				env.log.Info(fmt.Sprintf("Etcd subtree %s doesn't exist yet", key))
			} else {
				env.log.Error("Error fetching initial etcd subtree: ", err)
				return err
			}
		} else {
			etcd.FlattenNodes(resp.Node, &nodes)
		}
	}

	handleEtcdNode := func (action *string, node *etcdclient.Node) error {
		if node.Key == cellKey {
			return env.handleEtcdCellNode(action, node)
		} else if node.Key == cellNetKey {
			cellNetNodeExists = true
			return env.handleEtcdCellNetworkNode(action, node)
		} else if strings.HasPrefix(node.Key, etcd.APP_KEY_BASE + "/") {
			return env.handleEtcdAppNode(action, node)
		} else if strings.HasPrefix(node.Key, ctBaseKey) {
			return env.handleEtcdContainerNode(action, node)
		}
		return nil
	}

	act := "set"
	for _, nd := range nodes {
		handleEtcdNode(&act, nd)
	}
	if !cellNetNodeExists {
		env.log.Info("Cell network info node not found in etcd")
		defIpPool := env.getDefaultIpPool()
		env.agent.updateIpamAnnotation(defIpPool)
	}
	env.log.Debug(fmt.Sprintf("Handled %d initial etcd nodes", len(nodes)))

	watch_func := func(w etcdclient.Watcher) {
		for {
			resp, err := w.Next(context.Background())
			if err != nil {
				env.log.Error("Error in etcd watcher: ", err)
				return
			}
			// env.log.Debug("Etcd event: ", resp)
			handleEtcdNode(&resp.Action, resp.Node)
		}
	}
	// watch cells
	go watch_func(w)
	// watch apps
	go watch_func(app_w)
	return nil
}

func (env *CfEnvironment) handleEtcdCellNode(action *string, node *etcdclient.Node) error {
	if isDeleteAction(action) {
		env.agent.updateIpamAnnotation("[]")
	}
	return nil
}

func (env *CfEnvironment) handleEtcdCellNetworkNode(action *string, node *etcdclient.Node) error {
	if isDeleteAction(action) {
		env.agent.updateIpamAnnotation("[]")
	} else {
		env.agent.updateIpamAnnotation(node.Value)
	}
	return nil
}

func (env *CfEnvironment) handleEtcdContainerNode(action *string, node *etcdclient.Node) error {
	epNode := strings.HasSuffix(node.Key, "/ep")
	key_parts := strings.Split(node.Key, "/")
	ctId := key_parts[len(key_parts) - 2]
	if !epNode {
		ctId = key_parts[len(key_parts) - 1]
	}
	deleted := isDeleteAction(action)
	if epNode && !deleted {
		var ep etcd.EpInfo
		err := json.Unmarshal([]byte(node.Value), &ep)
		if err != nil {
			env.log.Error("Error deserializing container node value: ", err)
			return err
		}

		env.log.Info(fmt.Sprintf("Etcd udpate event for Container %s - %+v", ctId, ep))

		env.indexLock.Lock()
		env.epIdx[ctId] = ep
		env.indexLock.Unlock()

		env.cfAppContainerChanged(&ctId, &ep)
	}
	if deleted {
		env.log.Info("Etcd delete event for Container ", ctId)
		env.indexLock.Lock()
		ep, ok := env.epIdx[ctId]
		delete(env.epIdx, ctId)
		env.indexLock.Unlock()

		if ok {
			env.cfAppContainerDeleted(&ctId, &ep)
		} else {
			env.cfAppContainerDeleted(&ctId, nil)
		}
	}
	return nil
}

func (env *CfEnvironment) handleEtcdAppNode(action *string, node *etcdclient.Node) error {
	key_parts := strings.Split(node.Key, "/")
	appId := key_parts[len(key_parts) - 1]
	deleted := isDeleteAction(action)
	if !deleted {
		var app etcd.AppInfo
		err := json.Unmarshal([]byte(node.Value), &app)
		if err != nil {
			env.log.Error("Error deserializing app node value: ", err)
			return err
		}

		env.log.Info(fmt.Sprintf("Etcd udpate event for App %s - %+v", appId, app))
		env.indexLock.Lock()
		env.appIdx[appId] = app
		env.indexLock.Unlock()

		env.cfAppChanged(&appId, &app)
	} else {
		env.log.Info("Etcd delete event for App ", appId)
		env.indexLock.Lock()
		app, ok := env.appIdx[appId]
		delete(env.appIdx, appId)
		env.indexLock.Unlock()

		if ok {
			env.cfAppDeleted(&appId, &app)
		} else {
			env.cfAppDeleted(&appId, nil)
		}
	}
	return nil
}
