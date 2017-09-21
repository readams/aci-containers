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
	"net"
	"testing"

	"github.com/stretchr/testify/assert"
	"golang.org/x/net/context"

	"github.com/noironetworks/aci-containers/pkg/etcd"
	"github.com/noironetworks/aci-containers/pkg/ipam"
	"github.com/noironetworks/aci-containers/pkg/metadata"
)

func TestLoadCellNetworkInfo(t *testing.T) {
	env := testCfEnvironment(t)
	k := env.fakeEtcdKeysApi()
	cellId := "cell-1"
	key := etcd.CELL_KEY_BASE + "/" + cellId + "/network"
	ctx := context.Background()

	env.LoadCellNetworkInfo(cellId)
	_, ok := env.cont.nodePodNetCache[cellId]
	assert.False(t, ok)

	nodeMeta := newNodePodNetMeta()
	nodeMeta.podNetIps.V4 = append(nodeMeta.podNetIps.V4,
		ipam.IpRange{Start: net.ParseIP("10.10.0.1"), End: net.ParseIP("10.10.0.24")})
	nodeMeta.podNetIps.V6 = append(nodeMeta.podNetIps.V6,
		ipam.IpRange{Start: net.ParseIP("::fe80"), End: net.ParseIP("::fe90")})
	env.cont.recomputePodNetAnnotation(nodeMeta)

	k.Set(ctx, key, nodeMeta.podNetIpsAnnotation, nil)
	env.LoadCellNetworkInfo(cellId)
	r, ok := env.cont.nodePodNetCache[cellId]
	assert.True(t, ok)
	assert.Equal(t, nodeMeta.podNetIpsAnnotation, r.podNetIpsAnnotation)

	k.Delete(ctx, key, nil)
	env.LoadCellNetworkInfo(cellId)
	r, ok = env.cont.nodePodNetCache[cellId]
	assert.True(t, ok)
	assert.Equal(t, nodeMeta.podNetIpsAnnotation, r.podNetIpsAnnotation)
}

func TestLoadCellServiceInfo(t *testing.T) {
	env := testCfEnvironment(t)
	k := env.fakeEtcdKeysApi()
	cellId := "cell-1"
	nodename := "diego-cell-" + cellId
	key := etcd.CELL_KEY_BASE + "/" + cellId + "/service"
	ctx := context.Background()

	env.LoadCellServiceInfo(cellId)
	r, ok := env.cont.nodeServiceMetaCache[nodename]
	assert.True(t, ok)
	assert.NotEqual(t, "", r.serviceEp.Mac)
	assert.NotNil(t, r.serviceEp.Ipv4)
	assert.NotNil(t, r.serviceEp.Ipv6)

	svcEpStr, _ := json.Marshal(r.serviceEp)
	v, _ := k.Get(ctx, key, nil)
	assert.Equal(t, string(svcEpStr), v.Node.Value)

	delete(env.cont.nodeServiceMetaCache, nodename)
	svcEP := metadata.ServiceEndpoint{Mac: "aa:bb:cc:dd:ee:ff",
		Ipv4: net.ParseIP("1.0.0.10"),
		Ipv6: net.ParseIP("a1::1a"),
	}
	svcEpStr, _ = json.Marshal(&svcEP)
	k.Set(ctx, key, string(svcEpStr), nil)
	env.LoadCellServiceInfo(cellId)
	r, ok = env.cont.nodeServiceMetaCache[nodename]
	assert.True(t, ok)
	assert.Equal(t, "aa:bb:cc:dd:ee:ff", r.serviceEp.Mac)
	assert.Equal(t, net.ParseIP("1.0.0.10"), r.serviceEp.Ipv4)
	assert.Equal(t, net.ParseIP("a1::1a"), r.serviceEp.Ipv6)
	v, _ = k.Get(ctx, key, nil)
	assert.Equal(t, string(svcEpStr), v.Node.Value)

	k.Delete(ctx, key, nil)
	env.LoadCellServiceInfo(cellId)
	r, ok = env.cont.nodeServiceMetaCache[nodename]
	assert.True(t, ok)
	assert.Equal(t, "aa:bb:cc:dd:ee:ff", r.serviceEp.Mac)
	assert.Equal(t, net.ParseIP("1.0.0.10"), r.serviceEp.Ipv4)
	assert.Equal(t, net.ParseIP("a1::1a"), r.serviceEp.Ipv6)
}

func TestManageAppExtIp(t *testing.T) {
	env := testCfEnvironment(t)
	env.cont.staticServiceIps.V4.RemoveIp(net.ParseIP("1.2.3.2"))
	env.cont.staticServiceIps.V4.RemoveIp(net.ParseIP("1.2.3.3"))
	env.cont.staticServiceIps.V6.RemoveIp(net.ParseIP("::2f01"))
	env.cont.staticServiceIps.V6.RemoveIp(net.ParseIP("::2f02"))

	curr := []ExtIpAlloc{ExtIpAlloc{"1.2.3.2", false, ""},
		ExtIpAlloc{"1.2.3.3", false, ""},
		ExtIpAlloc{"::2f01", false, ""},
		ExtIpAlloc{"::2f02", false, ""},
	}

	// allocate static
	req := []ExtIpAlloc{ExtIpAlloc{"1.2.3.3", false, ""},
		ExtIpAlloc{"1.2.3.4", false, ""},
		ExtIpAlloc{"::2f02", false, ""},
		ExtIpAlloc{"::2f03", false, ""},
	}
	res, err := env.ManageAppExtIp(curr, req, false)
	assert.Nil(t, err)
	assert.Equal(t, req, res)
	assert.True(t, env.cont.staticServiceIps.V4.RemoveIp(net.ParseIP("1.2.3.2")))
	assert.True(t, env.cont.staticServiceIps.V6.RemoveIp(net.ParseIP("::2f01")))

	// allocate unavailable IP
	env.cont.staticServiceIps.V4.RemoveIp(net.ParseIP("1.2.3.6"))
	env.cont.staticServiceIps.V6.RemoveIp(net.ParseIP("::2f05"))
	v4copy := ipam.NewFromRanges(env.cont.staticServiceIps.V4.FreeList)
	v6copy := ipam.NewFromRanges(env.cont.staticServiceIps.V6.FreeList)
	req = []ExtIpAlloc{ExtIpAlloc{"1.2.3.5", false, ""},
		ExtIpAlloc{"1.2.3.6", false, ""},
		ExtIpAlloc{"::2f04", false, ""},
		ExtIpAlloc{"::2f05", false, ""},
	}
	res1, err := env.ManageAppExtIp(res, req, false)
	assert.NotNil(t, err)
	assert.Nil(t, res1)
	assert.Equal(t, v4copy, env.cont.staticServiceIps.V4)
	assert.Equal(t, v6copy, env.cont.staticServiceIps.V6)

	// deallocate static
	res1, err = env.ManageAppExtIp(res, nil, false)
	for _, ip := range []string{"1.2.3.3", "1.2.3.4"} {
		v4copy.AddIp(net.ParseIP(ip))
	}
	for _, ip := range []string{"::2f02", "::2f03"} {
		v6copy.AddIp(net.ParseIP(ip))
	}
	assert.Nil(t, err)
	assert.Nil(t, res1)
	assert.Equal(t, v4copy, env.cont.staticServiceIps.V4)
	assert.Equal(t, v6copy, env.cont.staticServiceIps.V6)

	// allocate dynamic
	res, err = env.ManageAppExtIp(res, nil, true)
	assert.Nil(t, err)
	assert.Equal(t, 2, len(res))
	assert.Equal(t, "1.2.4", res[0].IP[0:5])
	assert.Equal(t, "::2e0", res[1].IP[0:5])
	assert.False(t, env.cont.serviceIps.V4.RemoveIp(net.ParseIP(res[0].IP)))
	assert.False(t, env.cont.serviceIps.V6.RemoveIp(net.ParseIP(res[1].IP)))

	// allocate once again -> no-op
	v4copy = ipam.NewFromRanges(env.cont.serviceIps.V4.FreeList)
	v6copy = ipam.NewFromRanges(env.cont.serviceIps.V6.FreeList)
	res1, err = env.ManageAppExtIp(res, nil, true)
	assert.Nil(t, err)
	assert.Equal(t, 2, len(res1))
	assert.Equal(t, res, res1)
	assert.Equal(t, v4copy, env.cont.serviceIps.V4)
	assert.Equal(t, v6copy, env.cont.serviceIps.V6)

	// deallocate dynamic
	v4copy.AddIp(net.ParseIP(res1[0].IP))
	v6copy.AddIp(net.ParseIP(res1[1].IP))
	res1, err = env.ManageAppExtIp(res1, nil, false)
	assert.Nil(t, err)
	assert.Nil(t, res1)
	assert.Equal(t, v4copy, env.cont.serviceIps.V4)
	assert.Equal(t, v6copy, env.cont.serviceIps.V6)
}

func TestLoadAppExtIp(t *testing.T) {
	env := testCfEnvironment(t)
	ipdb := AppExtIpDb{}

	ip1 := ExtIpAlloc{"1.2.3.4", false, "p1"}
	ip2 := ExtIpAlloc{"1.2.4.4", true, "p2"}
	ip3 := ExtIpAlloc{"::2f02", false, "p1"}
	ip4 := ExtIpAlloc{"::2e01", true, "p1"}

	txn(env.db, func(txn *sql.Tx) {
		err := ipdb.Set(txn, "1", []ExtIpAlloc{ip1, ip3})
		assert.Nil(t, err)
		err = ipdb.Set(txn, "2", []ExtIpAlloc{ip2, ip4})
	})
	env.LoadAppExtIps()
	assert.False(t, env.cont.staticServiceIps.V4.RemoveIp(net.ParseIP("1.2.3.4")))
	assert.False(t, env.cont.staticServiceIps.V6.RemoveIp(net.ParseIP("::2f02")))

	assert.False(t, env.cont.serviceIps.V4.RemoveIp(net.ParseIP("1.2.4.4")))
	assert.False(t, env.cont.serviceIps.V6.RemoveIp(net.ParseIP("::2e01")))
}
