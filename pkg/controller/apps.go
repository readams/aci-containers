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
	"hash/fnv"
	"net"
	"strings"
	
	"code.cloudfoundry.org/bbs/events"
	"code.cloudfoundry.org/bbs/models"
	
	cfclient "github.com/cloudfoundry-community/go-cfclient"
	"golang.org/x/net/context"
	etcdclient "github.com/coreos/etcd/client"

	"github.com/noironetworks/aci-containers/pkg/apicapi"
	"github.com/noironetworks/aci-containers/pkg/etcd"
	"github.com/noironetworks/aci-containers/pkg/ipam"
)

type AppAndSpace struct {
	AppId       string          `json:"application_id",omitempty"`
	SpaceId     string          `json:"space_id",omitempty"`
}

type AppUpdateInfo struct {
	AppId       string          `json:"application_id",omitempty"`
	SpaceId     string          `json:"space_id",omitempty"`
	OrgId       string
	ContainerId string
	CellId      string
	Staging     bool
	Deleted     bool
}

func (env *CfEnvironment) initBbsEventListener(appCh chan<- AppUpdateInfo, stopCh <-chan struct{}) {
	env.initBbsAppListener(false, appCh, stopCh)
}

func (env *CfEnvironment) initBbsTaskListener(appCh chan<- AppUpdateInfo, stopCh <-chan struct{}) {
	env.initBbsAppListener(true, appCh, stopCh)
}

func findRunAction(act *models.Action) *models.RunAction {
	if act == nil {
		return nil
	}
	if act.RunAction != nil {
		return act.RunAction
	}
	var newacts []*models.Action
	if act.TimeoutAction != nil {
		newacts = append(newacts, act.TimeoutAction.Action)
	} else if act.EmitProgressAction != nil {
		newacts = append(newacts, act.EmitProgressAction.Action)
	} else if act.TryAction != nil {
		newacts = append(newacts, act.TryAction.Action)
	} else if act.ParallelAction != nil && len(act.ParallelAction.Actions) > 0 {
		newacts = act.ParallelAction.Actions
	} else if act.SerialAction != nil && len(act.SerialAction.Actions) > 0 {
		newacts = act.SerialAction.Actions
	} else if act.CodependentAction != nil && len(act.CodependentAction.Actions) > 0 {
		newacts = act.CodependentAction.Actions
	}
	for _, a := range newacts {
		ra := findRunAction(a)
		if ra != nil {
			return ra
		}
	}
	return nil
}

func (env *CfEnvironment) getAppAndSpaceFromLrp(dlrp *models.DesiredLRP) (string, string) {
	return env.getAppAndSpaceFromAction(dlrp.GetAction())
}

func (env *CfEnvironment) getAppAndSpaceFromTask(task *models.Task) (string, string) {
	return env.getAppAndSpaceFromAction(task.GetAction())
}

func (env *CfEnvironment) getAppAndSpaceFromAction(action *models.Action) (string, string) {
	act := findRunAction(action)
	if act != nil {
		for _, envVar := range act.Env {
			if envVar.Name == "VCAP_APPLICATION" {
				var as AppAndSpace
				err := json.Unmarshal([]byte(envVar.Value), &as)
				if err != nil {
					env.log.Error("JSON deserialize failed for VCAP_APPLICATION env var: ", err)
					return "", ""
				} else {
					return as.AppId, as.SpaceId
				}
			}
		}
	}
	return "", ""
}

func (env *CfEnvironment) fetchLrps(dlrpInfo map[string]AppAndSpace,
								   alrpInfo map[string]AppAndSpace,
								   pendingDlrp map[string][]models.ActualLRPInstanceKey,
								   appCh chan<- AppUpdateInfo) error {
	// Get all desired LRPs
	existDesired, err := env.bbsClient.DesiredLRPs(env.cfLogger, models.DesiredLRPFilter{})
	if err != nil {
		env.log.Error("Initial fetch of all desired LRPs failed: ", err)
		return err
	}
	for _, dlrp := range existDesired {
		appId, spaceId := env.getAppAndSpaceFromLrp(dlrp)
		if appId != "" && spaceId != "" {
			dlrpInfo[dlrp.GetProcessGuid()] = AppAndSpace{appId, spaceId}
		}
	}
	if err != nil {
		env.log.Error("Initial fetch of all actual LRPs failed: ", err)
		return err
	}
	// Get all actual LRPs
	existActualGroup, err := env.bbsClient.ActualLRPGroups(env.cfLogger, models.ActualLRPFilter{})
	for _, alrpg := range existActualGroup {
		upd := env.processActualLrp(alrpg, dlrpInfo, alrpInfo, pendingDlrp, false)
		if upd != nil {
			env.log.Debug(fmt.Sprintf("Sending initial app update: %+v", *upd))
			appCh <- *upd
		}
	}
	return nil
}

func (env *CfEnvironment) processActualLrp(alrpg *models.ActualLRPGroup,
										  dlrpInfo map[string]AppAndSpace,
										  alrpInfo map[string]AppAndSpace,
										  pendingDlrp map[string][]models.ActualLRPInstanceKey,
										  deleted bool) *AppUpdateInfo {
	var upd *AppUpdateInfo
	if alrpg.Instance != nil {
		pguid := alrpg.Instance.ProcessGuid
		contId := alrpg.Instance.InstanceGuid
		cellId := alrpg.Instance.CellId

		as, ok := dlrpInfo[pguid]
		if ok {
			upd = &AppUpdateInfo{AppId: as.AppId, SpaceId: as.SpaceId, Staging: false,
								CellId: cellId, ContainerId: contId,
								Deleted: deleted}
			if !deleted {
				alrpInfo[contId] = as
			}
		} else if !deleted {
			env.log.Debug("Pending LRP ProcessGuid " + pguid + ", containerId " + contId)
			pendingDlrp[pguid] = append(pendingDlrp[pguid],
									    models.ActualLRPInstanceKey{contId, cellId})
		}

		if deleted {
			if upd == nil {
				as := alrpInfo[contId]
				upd = &AppUpdateInfo{AppId: as.AppId, SpaceId: as.SpaceId,
									Staging: false, CellId: cellId, ContainerId: contId,
									Deleted: true}
			}
			delete(alrpInfo, pguid)

			// remove actual LRP from pending list
			pali := pendingDlrp[pguid]
			deleteIdx := -1
			for idx, lrpk := range pali {
				if lrpk.InstanceGuid == contId && lrpk.CellId == cellId {
					deleteIdx = idx
					break
				}
			}
			if deleteIdx != -1 {
				if len(pali) > 1 {
					pali[deleteIdx] = pali[len(pali)-1]
					pendingDlrp[pguid] = pali[:len(pali)-1]
				} else {
					delete(pendingDlrp, pguid)
				}
			}
		}
	}
	return upd
}

func (env *CfEnvironment) fetchTasks(appCh chan<- AppUpdateInfo) error {
	tasks, err := env.bbsClient.Tasks(env.cfLogger)
	if err != nil {
		env.log.Error("Initial fetch of all tasks failed: ", err)
		return err
	}
	for _, task := range tasks {
		appId, spaceId := env.getAppAndSpaceFromTask(task)
		if appId != "" && spaceId != "" {
			upd := AppUpdateInfo{AppId: appId, SpaceId: spaceId, Staging: true,
								ContainerId: task.TaskGuid, CellId: task.CellId,
								Deleted: false}
			appCh <- upd
		}
	}
	return nil
}

func (env *CfEnvironment) initBbsAppListener(isTask bool, appCh chan<- AppUpdateInfo, stopCh <-chan struct{}) {
	ev_type := "event"
	if isTask {
		ev_type = "task"
	}
	env.log.Info("Starting BBS " + ev_type + " listener")
	var err error
	var es events.EventSource
	if isTask {
		es, err = env.bbsClient.SubscribeToTaskEvents(env.cfLogger)
	} else {
		es, err = env.bbsClient.SubscribeToEvents(env.cfLogger)
	}
	if err != nil {
		env.log.Error("Unable to subscribe to BBS " + ev_type + ": ", err)
		return
	}
	
	dlrpInfo := make(map[string]AppAndSpace)
	alrpInfo := make(map[string]AppAndSpace)
	pendingDlrp := make(map[string][]models.ActualLRPInstanceKey)

	// Now that we have subscribed, fetch the current list of LRPs/tasks
	if isTask {
		err = env.fetchTasks(appCh)
	} else {
		err = env.fetchLrps(dlrpInfo, alrpInfo, pendingDlrp, appCh)
	}
	if err != nil {
		return
	}
	
	var event models.Event
	for {
		event, err = es.Next()
		if err != nil {
			switch err {
			case events.ErrUnrecognizedEventType:
				env.log.Error("failed-getting-next-event: ", err)
			default:
				env.log.Info("BBS " + ev_type + " EOF")
				return
			}
		}

		if event != nil {
			// env.log.Debug(ev_type + " received: " + event.EventType() + " for " + event.Key())
			deleted := false
			var dlrp *models.DesiredLRP
			var alrp *models.ActualLRPGroup
			var task *models.Task
			switch ev := event.(type) {
			case *models.DesiredLRPCreatedEvent:
				dlrp = ev.DesiredLrp
			case *models.DesiredLRPChangedEvent:
				dlrp = ev.After
			case *models.DesiredLRPRemovedEvent:
				dlrp = ev.DesiredLrp
				deleted = true

			case *models.ActualLRPCreatedEvent:
				alrp = ev.ActualLrpGroup
			case *models.ActualLRPChangedEvent:
				alrp = ev.After
			case *models.ActualLRPRemovedEvent:
				alrp = ev.ActualLrpGroup
				deleted = true

			case *models.TaskCreatedEvent:
				task = ev.Task
			case *models.TaskChangedEvent:
				task = ev.After
			case *models.TaskRemovedEvent:
				task = ev.Task
				deleted = true
			default:
				continue
			}
			var appId, spaceId string
			var appUpdates []AppUpdateInfo
			if dlrp != nil {
				appId, spaceId = env.getAppAndSpaceFromLrp(dlrp)
				if deleted {
					delete(dlrpInfo, dlrp.GetProcessGuid())
					delete(pendingDlrp, dlrp.GetProcessGuid())
				} else {
					dlrpInfo[dlrp.GetProcessGuid()] = AppAndSpace{appId, spaceId}
					for _, actual := range pendingDlrp[dlrp.GetProcessGuid()] {
						upd := AppUpdateInfo{AppId: appId, SpaceId: spaceId, Staging: false,
											 ContainerId: actual.InstanceGuid, CellId: actual.CellId,
											 Deleted: false}
						appUpdates = append(appUpdates, upd)
					}
					delete(pendingDlrp, dlrp.GetProcessGuid())
				}
			} else if alrp != nil {
				upd := env.processActualLrp(alrp, dlrpInfo, alrpInfo, pendingDlrp, deleted)
				if upd != nil {
					appUpdates = append(appUpdates, *upd)
				}
			} else if task != nil {
				appId, spaceId = env.getAppAndSpaceFromTask(task)
				if appId != "" && spaceId != "" {
					upd := AppUpdateInfo{AppId: appId, SpaceId: spaceId, Staging: true,
										 ContainerId: task.TaskGuid, CellId: task.CellId,
										 Deleted: deleted}
					appUpdates = append(appUpdates, upd)
				}
			}
			for _, upd := range appUpdates {
				env.log.Debug(fmt.Sprintf("Sending app update: %+v", upd))
				appCh <- upd
			}
		}
	}
}

type ContainerInfo struct {
	ContainerId     string
	CellId          string
	AppId           string
	Staging         bool
}

type AppInfo struct {
	AppId             string
	SpaceId           string
	NetworkPolicies   []string
}

type SpaceInfo struct {
	SpaceId                  string
	OrgId                    string
	RunningSecurityGroups    []string
	StagingSecurityGroups    []string
	IsolationSegment         string
}

type SecurityGroupInfo struct {
	cfclient.SecGroup
	RulesHash      string
}

func hashJsonSerializable(obj interface{}) (string, error) {
	js, err := json.Marshal(obj)
	if err != nil {
		return "", err
	}
	hasher := fnv.New64()
	hasher.Reset()
	_, err = hasher.Write(js)
	if err != nil {
		return "", err
	}
	return string(hasher.Sum64()), nil
}

func (env *CfEnvironment) initAppIndexBuilder(appCh <-chan AppUpdateInfo, stopCh <-chan struct{}) {
	updateAsg := func (sg *cfclient.SecGroup) bool {
		oldsg, ok := env.asgIdx[sg.Guid]
		// exclude these fields in hash calc
		sg.SpacesData = nil
		newHash, err := hashJsonSerializable(sg)
		if err != nil {
			env.log.Error("Failed to hash security group: ", err)
			return false
		}
		if !ok || oldsg.RulesHash != newHash {
			env.asgIdx[sg.Guid] = SecurityGroupInfo{*sg, newHash}
			return true
		}
		return false
	}

	// TODO cleanup stale entries from etcd

	for {
		select {
		case <-stopCh:
			return
		case upd := <-appCh:
			// env.log.Debug("Received app update: ", upd)
			if upd.ContainerId == "" {
				continue
			}
			
			var contChanged []string
			var contDeleted []*ContainerInfo
			var asgChanged []string
			
			env.indexLock.Lock()
			// TODO avoid holding lock while doing RPCs
			c, ok := env.contIdx[upd.ContainerId]
			if upd.Deleted {
				delete(env.contIdx, upd.ContainerId)
				if !ok {
					c.ContainerId = upd.ContainerId
				}
				contDeleted = append(contDeleted, &c)
			} else {
				if !ok {
					env.contIdx[upd.ContainerId] = ContainerInfo{ContainerId: upd.ContainerId,
																AppId: upd.AppId,
																CellId: upd.CellId,
																Staging: upd.Staging}
				} else {
					if upd.AppId != "" {
						c.AppId = upd.AppId
					}
					if upd.CellId != "" {
						c.CellId = upd.CellId
					}
					env.contIdx[upd.ContainerId] = c
				}
				contChanged = append(contChanged, upd.ContainerId)
				
				if upd.AppId != "" && upd.SpaceId != "" {
					env.appIdx[upd.AppId] = AppInfo{AppId: upd.AppId, SpaceId: upd.SpaceId}
				}
				if upd.SpaceId != "" {
					sp, err := env.ccClient.GetSpaceByGuid(upd.SpaceId)
					if err != nil {
						env.log.Error("Error fetching info for space " + upd.SpaceId + ": ", err)
						// how do we retry?
					}
					runsg, err := env.ccClient.ListSecGroupsBySpace(upd.SpaceId, false)
					if err != nil {
						env.log.Error("Error fetching running ASGs for space " + upd.SpaceId + ": ", err)
						// how do we retry?
					}
					stagesg, err := env.ccClient.ListSecGroupsBySpace(upd.SpaceId, true)
					if err != nil {
						env.log.Error("Error fetching staging ASGs for space " + upd.SpaceId + ": ", err)
						// how do we retry?
					}
					spi := SpaceInfo{SpaceId: sp.Guid, OrgId: sp.OrganizationGuid}
					for _, sg := range runsg {
						spi.RunningSecurityGroups = append(spi.RunningSecurityGroups, sg.Guid)
						if updateAsg(&sg) {
							asgChanged = append(asgChanged, sg.Guid)
						}
					}
					for _, sg := range stagesg {
						spi.StagingSecurityGroups = append(spi.StagingSecurityGroups, sg.Guid)
						if updateAsg(&sg) {
							asgChanged = append(asgChanged, sg.Guid)
						}
					}
					env.spaceIdx[upd.SpaceId] = spi
				}
			}
			env.indexLock.Unlock()

			for _, c := range contChanged {
				env.notifyContainerUpdate(c)
			}
			for _, c := range contDeleted {
				env.notifyContainerDelete(c)
			}
			for _, g := range asgChanged {
				env.notifyAsgUpdate(g)
			}
		}
	}
	return
}

func (env *CfEnvironment) notifyContainerUpdate(contId string) {
	cinfo, ok := env.contIdx[contId]
	if !ok || cinfo.AppId == "" {
		return
	}
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
		}
	}
	env.log.Debug(
		fmt.Sprintf("Container update: %s, cell %s, app %s", contId, cinfo.CellId, cinfo.AppId))
	if cinfo.CellId != "" {
		cellKey := etcd.CELL_KEY_BASE + "/" + cinfo.CellId
		netinfo := "[{\"start\": \"10.255.0.2\", \"end\": \"10.255.0.129\"}]"
		kapi := etcdclient.NewKeysAPI(env.etcdClient)
		_, err := kapi.Set(context.Background(), cellKey + "/network", netinfo, nil)
		if err != nil {
			env.log.Error("Error setting etcd net info for cell: ", err)
		}

		ctKey := etcd.CELL_KEY_BASE + "/" + cinfo.CellId + "/containers/" + cinfo.ContainerId
		ep := etcd.EpInfo{AppId: cinfo.AppId,
						  EpgTenant: env.cont.config.DefaultEg.PolicySpace,
						  Epg: env.cont.config.DefaultEg.Name}
		ep.SecurityGroups = append(ep.SecurityGroups,
								  etcd.GroupInfo{Group: env.cont.aciNameForKey("asg", "static"),
												 Tenant: env.cont.config.AciPolicyTenant})
		for _, s := range *sg {
			ep.SecurityGroups = append(ep.SecurityGroups,
									  etcd.GroupInfo{Group: env.cont.aciNameForKey("asg", s),
												  	 Tenant: env.cont.config.AciPolicyTenant})
		}
		ep_json, err := json.Marshal(ep)
		if err != nil {
			env.log.Error("Unable to serialize EP info: ", err)
		} else {
			_, err = kapi.Set(context.Background(), ctKey + "/ep", string(ep_json), nil)
			if err != nil {
				env.log.Error("Error setting container info: ", err)
			}
		}
	}
}

func (env *CfEnvironment) notifyContainerDelete(cinfo *ContainerInfo) {
	env.log.Debug(fmt.Sprintf("Container delete: %s, cell %s", cinfo.ContainerId, cinfo.CellId))
	if cinfo.CellId != "" {
		kapi := etcdclient.NewKeysAPI(env.etcdClient)
		ctKey := etcd.CELL_KEY_BASE + "/" + cinfo.CellId + "/containers/" + cinfo.ContainerId
		_, err := kapi.Delete(context.Background(), ctKey, &etcdclient.DeleteOptions{Recursive: true})
		if err != nil {
			env.log.Error("Error deleting container node: ", err)
		}
	}
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

func (env *CfEnvironment) notifyAsgUpdate(asgId string) {
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
