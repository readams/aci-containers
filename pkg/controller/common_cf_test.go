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
	"fmt"
	"net"
	"sync"
	"testing"

	etcdclient "github.com/coreos/etcd/client"
	_ "github.com/mattn/go-sqlite3"
	"github.com/Sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"golang.org/x/net/context"

	"github.com/noironetworks/aci-containers/pkg/cfapi"
	"github.com/noironetworks/aci-containers/pkg/etcd"
)

func testCfEnvironmentNoMigration(t *testing.T) *CfEnvironment {
	env := CfEnvironment{indexLock: &sync.Mutex{}}
	log := logrus.New()
	log.Level = logrus.DebugLevel
	log.Formatter = &logrus.TextFormatter{
		DisableColors: true,
	}
	cont := NewController(NewConfig(), &env, log)
	cont.config.DefaultEg.Name = "default|cf-app-default"
	cont.config.DefaultEg.PolicySpace = "cf"
	cont.configuredPodNetworkIps.V4.AddRange(net.ParseIP("10.10.0.0"), net.ParseIP("10.10.255.255"))
	cont.configuredPodNetworkIps.V6.AddRange(net.ParseIP("::fe00"), net.ParseIP("::feff"))
	cont.nodeServiceIps.V4.AddRange(net.ParseIP("1.0.0.1"), net.ParseIP("1.0.0.24"))
	cont.nodeServiceIps.V6.AddRange(net.ParseIP("a1::"), net.ParseIP("a1::ff"))
	cont.staticServiceIps.V4.AddRange(net.ParseIP("1.2.3.1"), net.ParseIP("1.2.3.20"))
	cont.staticServiceIps.V6.AddRange(net.ParseIP("::2f00"), net.ParseIP("::2fff"))
	cont.serviceIps.V4.AddRange(net.ParseIP("1.2.4.1"), net.ParseIP("1.2.4.20"))
	cont.serviceIps.V6.AddRange(net.ParseIP("::2e00"), net.ParseIP("::2eff"))
	env.cont = cont
	env.log = log
	db, err := sql.Open("sqlite3", ":memory:")
	db.SetMaxOpenConns(1)         // workaround for sqlite3 memory-only DB
	assert.Nil(t, err)
	env.db = db
	env.cfconfig = &CfConfig{}
	env.cfconfig.ApiPathPrefix = "/networking-aci"
	env.cfconfig.DefaultAppProfile = "auto"
	env.ccClient = &cfapi.FakeCcClient{}

	admin_token := cfapi.TokenInfo{Scope: []string{"network.admin"},
								  UserId: "admin",
								  UserName: "admin"}
	auth := cfapi.FakeCfAuthClient{}
	auth.FetchTokenInfo_response = &admin_token
	env.cfAuthClient = &auth
	env.etcdKeysApi = &fakeEtcdKeysApi{data: make(map[string]string), log: log}
	env.setupIndexes()
	return &env
}

func testCfEnvironment(t *testing.T) *CfEnvironment {
	env := testCfEnvironmentNoMigration(t)
	err := env.RunDbMigration()
	assert.Nil(t, err)
	return env
}

func (e *CfEnvironment) fakeCcClient() *cfapi.FakeCcClient {
	return e.ccClient.(*cfapi.FakeCcClient)
}

func (e *CfEnvironment) fakeCfAuthClient() *cfapi.FakeCfAuthClient {
	return e.cfAuthClient.(*cfapi.FakeCfAuthClient)
}

func (e *CfEnvironment) fakeEtcdKeysApi() *fakeEtcdKeysApi {
	return e.etcdKeysApi.(*fakeEtcdKeysApi)
}

func (e *CfEnvironment) setupIndexes() {
	e.contIdx = make(map[string]ContainerInfo)
	e.contIdx["c-1"] = ContainerInfo{ContainerId: "c-1", CellId: "cell-1", IpAddress: "1.2.3.4", AppId: "app-1"}
	e.contIdx["c-2"] = ContainerInfo{ContainerId: "c-2", CellId: "cell-1", IpAddress: "1.2.3.5", AppId: "app-1"}
	e.contIdx["c-3"] = ContainerInfo{ContainerId: "c-3", CellId: "cell-1", IpAddress: "1.2.3.6", AppId: "app-2"}
	e.contIdx["c-4"] = ContainerInfo{ContainerId: "c-4", CellId: "cell-1", IpAddress: "1.2.3.7", AppId: "app-3"}

	e.appIdx = make(map[string]AppInfo)
	e.appIdx["app-1"] = AppInfo{AppId: "app-1", SpaceId: "space-1",
		ContainerIps: map[string]string {
			"c-1": "1.2.3.4",
			"c-2": "1.2.3.5",
		},
	}
	e.appIdx["app-2"] = AppInfo{AppId: "app-2", SpaceId: "space-1",
		ContainerIps: map[string]string {"c-3": "1.2.3.6"},
	}
	e.appIdx["app-3"] = AppInfo{AppId: "app-3", SpaceId: "space-2",
		ContainerIps: map[string]string {"c-4": "1.2.3.7"},
	}
	e.spaceIdx = make(map[string]SpaceInfo)
	e.spaceIdx["space-1"] = SpaceInfo{SpaceId: "space-1", OrgId: "org-1"}
	e.spaceIdx["space-2"] = SpaceInfo{SpaceId: "space-2", OrgId: "org-1"}
}

type fakeEtcdKeysApi struct {
	data         map[string]string
	log          *logrus.Logger
}


func (k *fakeEtcdKeysApi) Get(ctx context.Context, key string, opts *etcdclient.GetOptions) (*etcdclient.Response, error) {
	v, ok := k.data[key]
	if ok {
		return &etcdclient.Response{Node: &etcdclient.Node{Value: v}}, nil
	}
	return nil, etcdclient.Error{Code: etcdclient.ErrorCodeKeyNotFound}
}

func (k *fakeEtcdKeysApi) Set(ctx context.Context, key, value string, opts *etcdclient.SetOptions) (*etcdclient.Response, error) {
	k.log.Debug(fmt.Sprintf("Setting %s = %s", key, value))
	k.data[key] = value
	return nil, nil
}

func (k *fakeEtcdKeysApi) Delete(ctx context.Context, key string, opts *etcdclient.DeleteOptions) (*etcdclient.Response, error) {
	delete(k.data, key)
	return nil, nil
}

func (k *fakeEtcdKeysApi) Create(ctx context.Context, key, value string) (*etcdclient.Response, error) {
	return k.Set(ctx, key, value, nil)
}

func (k *fakeEtcdKeysApi) CreateInOrder(ctx context.Context, dir, value string,
	                                    opts *etcdclient.CreateInOrderOptions) (*etcdclient.Response, error) {
	return nil, fmt.Errorf("Not Implemented")
}

func (k *fakeEtcdKeysApi) Update(ctx context.Context, key, value string) (*etcdclient.Response, error) {
	return k.Set(ctx, key, value, nil)
}

func (k *fakeEtcdKeysApi) Watcher(key string, opts *etcdclient.WatcherOptions) etcdclient.Watcher {
	return nil
}

func (k *fakeEtcdKeysApi) Equals(key string, expectedValue interface{}) bool {
	str, ok := expectedValue.(string)
	if !ok {
		bytes, err := json.Marshal(expectedValue)
		if err != nil {
			panic(err.Error())
		}
		str = string(bytes)
	}
	found, ok := k.data[key]
	return ok && found == str
}

func (k *fakeEtcdKeysApi) GetEpInfo(cell, cont string) *etcd.EpInfo {
	key := etcd.CELL_KEY_BASE + "/" + cell + "/containers/" + cont + "/ep"
	found, ok := k.data[key]
	if ok {
		var ep etcd.EpInfo
		er := json.Unmarshal([]byte(found), &ep)
		if er != nil {
			panic(er.Error())
		}
		return &ep
	}
	return nil
}

func (k *fakeEtcdKeysApi) GetAppInfo(appId string) *etcd.AppInfo {
	key := etcd.APP_KEY_BASE + "/" + appId
	found, ok := k.data[key]
	if ok {
		var app etcd.AppInfo
		er := json.Unmarshal([]byte(found), &app)
		if er != nil {
			panic(er.Error())
		}
		return &app
	}
	return nil
}

func txn(db *sql.DB, f func (txn *sql.Tx)) {
	txn, _ := db.Begin()
	defer txn.Commit()
	f(txn)
}