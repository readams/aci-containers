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
	"strings"
	
	"golang.org/x/net/context"
	etcdclient "github.com/coreos/etcd/client"
	
	"github.com/noironetworks/aci-containers/pkg/etcd"
)

func (env *CfEnvironment) processResponse(nd *etcdclient.Node, nodes *etcdclient.Nodes) {
	if nd == nil {
		return
	}
	
	*nodes = append(*nodes, nd)
	for _, n := range nd.Nodes {
		env.processResponse(n, nodes)
	}
}

func (env *CfEnvironment) initEtcdListener(stopCh <-chan struct{}) error {
	kapi := etcdclient.NewKeysAPI(env.etcdClient)
	cellKey := etcd.CELL_KEY_BASE + "/" + env.cellID
	ctBaseKey := cellKey + "/containers/"
	w := kapi.Watcher(cellKey, &etcdclient.WatcherOptions{Recursive: true})

	// Get info about the cell-specific subtree
	resp, err := kapi.Get(context.Background(), cellKey, &etcdclient.GetOptions{Recursive: true})
	var nodes etcdclient.Nodes
	if err != nil {
		keyerr, ok := err.(etcdclient.Error)
		if ok && keyerr.Code == etcdclient.ErrorCodeKeyNotFound {
			env.log.Info(fmt.Sprintf("Etcd subtree %s doesn't exist yet", cellKey))
		} else {
			env.log.Error("Error fetching initial etcd subtree for cell: ", err)
			return err
		}
	} else {
		env.processResponse(resp.Node, &nodes)
	}

	handleEtcdNode := func (action *string, node *etcdclient.Node) error {
		if strings.HasPrefix(node.Key, ctBaseKey) {
			return env.handleEtcdContainerNode(action, node)
		}
		return nil
	}

	act := "set"
	for _, nd := range nodes {
		handleEtcdNode(&act, nd)
	}
	env.log.Debug(fmt.Sprintf("Handled %d initial etcd nodes", len(nodes)))

	go func() {
		for {
			resp, err := w.Next(context.Background())
			if err != nil {
				env.log.Error("Error in etcd watcher: ", err)
				return
			}
			// env.log.Debug("Etcd event: ", resp)
			handleEtcdNode(&resp.Action, resp.Node)
		}
	}()
	return nil
}

func (env *CfEnvironment) handleEtcdContainerNode(action *string, node *etcdclient.Node) error {
	epNode := strings.HasSuffix(node.Key, "/ep")
	key_parts := strings.Split(node.Key, "/")
	ctId := key_parts[len(key_parts) - 2]
	if !epNode {
		ctId = key_parts[len(key_parts) - 1]
	}
	deleted := (*action == "delete" || *action == "compareAndDelete" || *action == "expire")
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

		metaKey := "_cf_/" + ctId
		env.cfAppChanged(&ctId, &metaKey)
	}
	if deleted {
		env.log.Info("Etcd delete event for Container ", ctId)
		env.indexLock.Lock()
		delete(env.epIdx, ctId)
		env.indexLock.Unlock()

		env.cfAppDeleted(&ctId)
	}
	return nil
}