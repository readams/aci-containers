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
	"fmt"
	"net"
	"strings"

	cfclient "github.com/cloudfoundry-community/go-cfclient"
	etcdclient "github.com/coreos/etcd/client"
	"golang.org/x/net/context"

	"github.com/noironetworks/aci-containers/pkg/apicapi"
	"github.com/noironetworks/aci-containers/pkg/etcd"
	"github.com/noironetworks/aci-containers/pkg/ipam"
)

func (env *CfEnvironment) handleContainerUpdateLocked(contId string) {
	cinfo, ok := env.contIdx[contId]
	if !ok || cinfo.AppId == "" {
		return
	}
	epgTenant := env.cont.config.DefaultEg.PolicySpace
	epg := env.cont.config.DefaultEg.Name

	appInfo, ok := env.appIdx[cinfo.AppId]
	var sg *[]string
	if ok && appInfo.SpaceId != "" {
		spaceInfo, ok := env.spaceIdx[appInfo.SpaceId]
		if ok {
			if cinfo.Staging {
				sg = &spaceInfo.StagingSecurityGroups
			} else {
				sg = &spaceInfo.RunningSecurityGroups
			}
			if is := env.isoSegIdx[spaceInfo.IsolationSegment].Name; is != "" {
				epg = env.cfconfig.DefaultAppProfile + "|" + is
			} else {
				epg_ann_db := EpgAnnotationDb{}
				txn, _ := env.db.Begin()
				v, err := epg_ann_db.ResolveAnnotation(txn, cinfo.AppId,
													  appInfo.SpaceId, spaceInfo.OrgId)
				if err != nil {
					env.log.Error("Failed to resolve EPG annotation: ", err)
				} else if v != "" {
					if strings.Contains(v, "|") {
						epg = v
					} else {
						epg = env.cfconfig.DefaultAppProfile + "|" + v
					}
				}
				txn.Commit()
			}
		}
	}
	env.log.Debug(fmt.Sprintf("Container update: %+v", cinfo))
	if cinfo.CellId != "" {

		env.cont.indexMutex.Lock()
		env.cont.addPodToNode(cinfo.CellId, contId)
		env.cont.indexMutex.Unlock()

		ctKey := etcd.CELL_KEY_BASE + "/" + cinfo.CellId + "/containers/" + cinfo.ContainerId
		ep := etcd.EpInfo{AppId: cinfo.AppId,
						  AppName: appInfo.AppName,
						  InstanceIndex: cinfo.InstanceIndex,
						  IpAddress: cinfo.IpAddress,
						  EpgTenant: epgTenant,
						  Epg: epg}
		for _, pm := range cinfo.Ports {
			ep.PortMapping = append(ep.PortMapping,
								    etcd.PortMap{ContainerPort: pm.ContainerPort, HostPort: pm.HostPort})
		}
		ep.SecurityGroups = append(ep.SecurityGroups,
								  etcd.GroupInfo{Group: env.cont.aciNameForKey("hpp", "static"),
												 Tenant: env.cont.config.AciPolicyTenant})
		if sg != nil {
			for _, s := range *sg {
				ep.SecurityGroups = append(ep.SecurityGroups,
										  etcd.GroupInfo{Group: env.cont.aciNameForKey("asg", s),
														 Tenant: env.cont.config.AciPolicyTenant})
			}
		}
		_, ok := env.netpolIdx[cinfo.AppId]
		if ok {
			ep.SecurityGroups = append(ep.SecurityGroups,
									  etcd.GroupInfo{Group: env.cont.aciNameForKey("np", cinfo.AppId),
													 Tenant: env.cont.config.AciPolicyTenant})
		}
		ep_json, err := json.Marshal(ep)
		if err != nil {
			env.log.Error("Unable to serialize EP info: ", err)
		} else {
			kapi := env.etcdKeysApi
			_, err = kapi.Set(context.Background(), ctKey + "/ep", string(ep_json), nil)
			if err != nil {
				env.log.Error("Error setting container info: ", err)
			}
		}
	}
}

func (env *CfEnvironment) handleContainerDeleteLocked(cinfo *ContainerInfo) {
	env.log.Debug(fmt.Sprintf("Container delete: %+v", *cinfo))
	if cinfo.CellId != "" {
		env.cont.indexMutex.Lock()
		env.cont.removePodFromNode(cinfo.CellId, cinfo.ContainerId)
		env.cont.indexMutex.Unlock()

		kapi := env.etcdKeysApi
		ctKey := etcd.CELL_KEY_BASE + "/" + cinfo.CellId + "/containers/" + cinfo.ContainerId
		_, err := kapi.Delete(context.Background(), ctKey, &etcdclient.DeleteOptions{Recursive: true})
		if err != nil {
			env.log.Error("Error deleting container node: ", err)
		}
	}
}

func (env *CfEnvironment) handleAppUpdateLocked(appId string) {
	ainfo, ok := env.appIdx[appId]
	if !ok {
		return
	}
	env.log.Debug(fmt.Sprintf("App update : %+v", ainfo))
	// find destination PolIds for which this App is a source
	var dstPolIds []string
	for k, info := range env.netpolIdx {
		if _, ok := info[appId]; ok {
			dstPolIds = append(dstPolIds, k)
		}
	}
	env.log.Debug("Dst policy Ids to update: ", dstPolIds)
	for _, d := range dstPolIds {
		hpp := env.createHppForNetPol(&d)
		env.cont.apicConn.WriteApicObjects("np:" + d, hpp)
	}
}

func (env *CfEnvironment) updateAppContainersLocked(appId string) {
	for k := range env.appIdx[appId].ContainerIps {
		env.handleContainerUpdateLocked(k)
	}
}

func (env *CfEnvironment) updateAppContainers(appId string) {
	env.indexLock.Lock()
	defer env.indexLock.Unlock()
	env.updateAppContainersLocked(appId)
}

func (env *CfEnvironment) updateSpaceContainersLocked(spaceId string) {
	for id, a := range env.appIdx {
		if a.SpaceId == spaceId {
			env.updateAppContainersLocked(id)
		}
	}
}

func (env *CfEnvironment) updateSpaceContainers(spaceId string) {
	env.indexLock.Lock()
	defer env.indexLock.Unlock()
	env.updateSpaceContainersLocked(spaceId)
}

func (env *CfEnvironment) updateOrgContainersLocked(orgId string) {
	for id, s := range env.spaceIdx {
		if s.OrgId == orgId {
			env.updateSpaceContainersLocked(id)
		}
	}
}

func (env *CfEnvironment) updateOrgContainers(orgId string) {
	env.indexLock.Lock()
	defer env.indexLock.Unlock()
	env.updateOrgContainersLocked(orgId)
}

type Range struct {
	start string
	end string
}

func splitIntoRanges(input *string) []Range {
	var ranges []Range
	if *input == "" {
		return ranges
	}
	for _, cp := range strings.Split(*input, ",") {
		parts := strings.Split(cp, "-")
		if len(parts) <= 2 {
			ranges = append(ranges, Range{start: strings.TrimSpace(parts[0]),
										 end: strings.TrimSpace(parts[len(parts) - 1])})
		}
	}
	return ranges
}

func convertAsgRule(rule *cfclient.SecGroupRule, parentDn *string, baseName *string) apicapi.ApicSlice {
	var remotes []*net.IPNet
	if rule.Destination == "" {
		remotes = append(remotes, &net.IPNet{IP: net.IPv4(0, 0, 0, 0), Mask: net.IPv4Mask(0, 0, 0, 0)})
	} else {
		dsts := splitIntoRanges(&rule.Destination)
		for _, d := range dsts {
			if strings.Contains(d.start, "/") {
				_, n, _ := net.ParseCIDR(d.start)
				remotes = append(remotes, n)
			} else {
				for _, n := range ipam.Range2Cidr(net.ParseIP(d.start), net.ParseIP(d.end)) {
					remotes = append(remotes, n)
				}
			}
		}
	}

	var ports []Range
	if rule.Ports == "" {
		ports = []Range{Range{start: "unspecified", end: "unspecified"}}
	} else {
		ports = splitIntoRanges(&rule.Ports)
	}

	proto := "unspecified"
	if rule.Protocol == "tcp" || rule.Protocol == "udp" || rule.Protocol == "icmp" {
		proto = rule.Protocol
	}
	// TODO convert ICMP type and ICMP code, and possibly Log

	var hprs apicapi.ApicSlice
	for pi, port := range ports {
		hpr := apicapi.NewHostprotRule(*parentDn, fmt.Sprintf("%s_%d", *baseName, pi))
		hpr.SetAttr("direction", "egress")
		hpr.SetAttr("ethertype", "ipv4")              // TODO use dst address
		hpr.SetAttr("protocol", proto)
		hpr.SetAttr("fromPort", port.start)
		hpr.SetAttr("toPort", port.end)
		for _, r := range remotes {
			hpremote := apicapi.NewHostprotRemoteIp(hpr.GetDn(), r.String())
			hpr.AddChild(hpremote)
		}
		hprs = append(hprs, hpr)
	}
	return hprs
}

func (env *CfEnvironment) handleAsgUpdateLocked(asgId string) {
	sginfo, ok := env.asgIdx[asgId]
	if !ok {
		return
	}
	env.log.Debug(fmt.Sprintf("ASG update %s: rules %v", asgId, sginfo.Rules))

	cont := env.cont

	asgApicName := cont.aciNameForKey("asg", asgId)
	hpp := apicapi.NewHostprotPol(cont.config.AciPolicyTenant, asgApicName)
	hpp.SetAttr("nameAlias", "asg_" + sginfo.Name)
	egressSubj := apicapi.NewHostprotSubj(hpp.GetDn(), "egress")
	subjDn := egressSubj.GetDn()
	for ri, rule := range sginfo.Rules {
		baseName := fmt.Sprintf("rule%d", ri)
		for _, hpr := range convertAsgRule(&rule, &subjDn, &baseName) {
			egressSubj.AddChild(hpr)
		}
	}
	hpp.AddChild(egressSubj)

	cont.apicConn.WriteApicObjects("asg:" + asgId, apicapi.ApicSlice{hpp})
}
