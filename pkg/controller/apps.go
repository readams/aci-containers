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
	"strings"
	"reflect"
	"time"

	"code.cloudfoundry.org/bbs/events"
	"code.cloudfoundry.org/bbs/models"

	cfclient "github.com/cloudfoundry-community/go-cfclient"
	etcdclient "github.com/coreos/etcd/client"
	"golang.org/x/net/context"

	"github.com/noironetworks/aci-containers/pkg/apicapi"
	"github.com/noironetworks/aci-containers/pkg/cfapi"
	"github.com/noironetworks/aci-containers/pkg/etcd"
)

type AppAndSpace struct {
	AppId           string          `json:"application_id",omitempty"`
	SpaceId         string          `json:"space_id",omitempty"`
	AppName         string          `json:"application_name",omitempty"`
}

type AppUpdateInfo struct {
	AppId           string
	AppName         string
	SpaceId         string
	OrgId           string
	ContainerId     string
	InstanceIndex   int32
	IpAddress       string
	Ports           []models.PortMapping
	CellId          string
	Staging         bool
	Deleted         bool
}

type ActualLRPInfo struct {
    InstanceGuid string
    CellId       string
    IpAddress    string
    Index        int32
    Ports        []models.PortMapping
}

func (env *CfEnvironment) initBbsEventListener(appCh chan<- interface{}, stopCh <-chan struct{}) {
	env.initBbsAppListener(false, appCh, stopCh)
}

func (env *CfEnvironment) initBbsTaskListener(appCh chan<- interface{}, stopCh <-chan struct{}) {
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

func (env *CfEnvironment) getAppAndSpaceFromLrp(dlrp *models.DesiredLRP) *AppAndSpace {
	return env.getAppAndSpaceFromAction(dlrp.GetAction())
}

func (env *CfEnvironment) getAppAndSpaceFromTask(task *models.Task) *AppAndSpace {
	return env.getAppAndSpaceFromAction(task.GetAction())
}

func (env *CfEnvironment) getAppAndSpaceFromAction(action *models.Action) *AppAndSpace {
	act := findRunAction(action)
	if act != nil {
		for _, envVar := range act.Env {
			if envVar.Name == "VCAP_APPLICATION" {
				var as AppAndSpace
				err := json.Unmarshal([]byte(envVar.Value), &as)
				if err != nil {
					env.log.Error("JSON deserialize failed for VCAP_APPLICATION env var: ", err)
					return nil
				} else {
					return &as
				}
			}
		}
	}
	return nil
}

func (env *CfEnvironment) fetchLrps(dlrpInfo map[string]AppAndSpace,
								   alrpInfo map[string]AppAndSpace,
								   pendingDlrp map[string][]ActualLRPInfo,
								   appCh chan<- interface{}) error {
	// Get all desired LRPs
	existDesired, err := env.bbsClient.DesiredLRPs(env.cfLogger, models.DesiredLRPFilter{})
	if err != nil {
		env.log.Error("Initial fetch of all desired LRPs failed: ", err)
		return err
	}
	for _, dlrp := range existDesired {
		as := env.getAppAndSpaceFromLrp(dlrp)
		if as != nil {
			dlrpInfo[dlrp.GetProcessGuid()] = *as
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
										  pendingDlrp map[string][]ActualLRPInfo,
										  deleted bool) *AppUpdateInfo {
	var upd *AppUpdateInfo
	if alrpg.Instance != nil {
		pguid := alrpg.Instance.ProcessGuid
		contId := alrpg.Instance.InstanceGuid
		cellId := alrpg.Instance.CellId
		addr := alrpg.Instance.InstanceAddress
		index := alrpg.Instance.Index
		ports := make([]models.PortMapping, 0, len(alrpg.Instance.Ports))
		for _, p := range alrpg.Instance.Ports {
			ports = append(ports, *p)
		}

		as, ok := dlrpInfo[pguid]
		if ok {
			upd = &AppUpdateInfo{AppId: as.AppId, SpaceId: as.SpaceId, Staging: false,
								CellId: cellId, ContainerId: contId, IpAddress: addr,
								AppName: as.AppName, InstanceIndex: index, Ports: ports,
								Deleted: deleted}
			if !deleted {
				alrpInfo[contId] = as
			}
		} else if !deleted {
			env.log.Debug("Pending LRP ProcessGuid " + pguid + ", containerId " + contId)
			pendingDlrp[pguid] = append(pendingDlrp[pguid],
									    ActualLRPInfo{contId, cellId, addr, index, ports})
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

func (env *CfEnvironment) fetchTasks(appCh chan<- interface{}) error {
	tasks, err := env.bbsClient.Tasks(env.cfLogger)
	if err != nil {
		env.log.Error("Initial fetch of all tasks failed: ", err)
		return err
	}
	for _, task := range tasks {
		as := env.getAppAndSpaceFromTask(task)
		if as != nil {
			upd := AppUpdateInfo{AppId: as.AppId, SpaceId: as.SpaceId, Staging: true,
								ContainerId: task.TaskGuid, CellId: task.CellId,
								AppName: as.AppName, InstanceIndex: -1,
								Deleted: false}
			appCh <- upd
		}
	}
	return nil
}

func (env *CfEnvironment) initBbsAppListener(isTask bool, appCh chan<- interface{}, stopCh <-chan struct{}) {
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
	pendingDlrp := make(map[string][]ActualLRPInfo)

	// Now that we have subscribed, fetch the current list of LRPs/tasks
	if isTask {
		err = env.fetchTasks(appCh)
	} else {
		err = env.fetchLrps(dlrpInfo, alrpInfo, pendingDlrp, appCh)
		if err == nil {
			appCh <- "bbs-ready"
		}
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
			var appUpdates []AppUpdateInfo
			if dlrp != nil {
				as := env.getAppAndSpaceFromLrp(dlrp)
				if deleted {
					delete(dlrpInfo, dlrp.GetProcessGuid())
					delete(pendingDlrp, dlrp.GetProcessGuid())
				} else if as != nil {
					dlrpInfo[dlrp.GetProcessGuid()] = *as
					for _, actual := range pendingDlrp[dlrp.GetProcessGuid()] {
						upd := AppUpdateInfo{AppId: as.AppId, SpaceId: as.SpaceId, Staging: false,
											 ContainerId: actual.InstanceGuid, CellId: actual.CellId,
											 IpAddress: actual.IpAddress, Deleted: false,
											 InstanceIndex: actual.Index, Ports: actual.Ports,
											 AppName: as.AppName}
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
				as := env.getAppAndSpaceFromTask(task)
				if as != nil {
					upd := AppUpdateInfo{AppId: as.AppId, SpaceId: as.SpaceId, Staging: true,
										 ContainerId: task.TaskGuid, CellId: task.CellId,
										 AppName: as.AppName, InstanceIndex: -1,
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
	InstanceIndex   int32
	IpAddress       string
	Ports           []models.PortMapping
	AppId           string
	Staging         bool
}

type AppInfo struct {
	AppId             string
	SpaceId           string
	AppName           string
	ContainerIps      map[string]string
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
	RulesHash      uint64
}

type IsoSegInfo struct {
	Id              string
	Name            string
}

func hashJsonSerializable(obj interface{}) (uint64, error) {
	js, err := json.Marshal(obj)
	if err != nil {
		return 0, err
	}
	hasher := fnv.New64()
	hasher.Reset()
	_, err = hasher.Write(js)
	if err != nil {
		return 0, err
	}
	return hasher.Sum64(), nil
}

func (env *CfEnvironment) fetchSpaceInfo(spaceId *string) (*SpaceInfo, []cfclient.SecGroup, error) {
	sp, err := env.ccClient.GetSpaceByGuid(*spaceId)
	if err != nil {
		env.log.Error("Error fetching info for space " + *spaceId + ": ", err)
		return nil, nil, err
	}
	spi := SpaceInfo{SpaceId: sp.Guid, OrgId: sp.OrganizationGuid}

	// fetch isolation segment info
	isoseg, err := env.ccClient.GetSpaceIsolationSegment(sp.Guid)
	if err != nil {
		env.log.Error("Error fetching isolation segment for space " + *spaceId + ": ", err)
		return &spi, nil, err
	}
	if isoseg == "" {
		isoseg, err = env.ccClient.GetOrgDefaultIsolationSegment(sp.OrganizationGuid)
		if err != nil {
			env.log.Error("Error fetching default segment for org " + sp.OrganizationGuid + ": ", err)
			return &spi, nil, err
		}
	}
	spi.IsolationSegment = isoseg

	// fetch ASG info
	runsg, err := env.ccClient.ListSecGroupsBySpace(*spaceId, false)
	if err != nil {
		env.log.Error("Error fetching running ASGs for space " + *spaceId + ": ", err)
		return &spi, nil, err
	}
	stagesg, err := env.ccClient.ListSecGroupsBySpace(*spaceId, true)
	if err != nil {
		env.log.Error("Error fetching staging ASGs for space " + *spaceId + ": ", err)
		return &spi, nil, err
	}
	for _, sg := range runsg {
		spi.RunningSecurityGroups = append(spi.RunningSecurityGroups, sg.Guid)
	}
	for _, sg := range stagesg {
		spi.StagingSecurityGroups = append(spi.StagingSecurityGroups, sg.Guid)
	}
	var allsgs []cfclient.SecGroup
	allsgs = append(allsgs, runsg...)
	allsgs = append(allsgs, stagesg...)
	return &spi, allsgs, nil
}

func (env *CfEnvironment) initAppIndexBuilder(appCh <-chan interface{}, stopCh <-chan struct{}) {
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

	for {
		select {
		case <-stopCh:
			return

		case recvd:= <- appCh:
			str, ok := recvd.(string)
			if ok {
				if str == "bbs-ready" {
					// we have received all app-updates for existing apps. Cleanup stale etcd entries
					env.log.Debug("Cleaning up stale etcd entries")
					err := env.cleanupEtcdContainers()
					if err != nil {
						env.log.Warning("Error cleaning up stale etcd container nodes: ", err)
					}
					env.idxStatusChan <- "index-ready"
				}
				continue
			}
			upd, ok := recvd.(AppUpdateInfo)
			if !ok || upd.ContainerId == "" {
				continue
			}
			// env.log.Debug("Received app update: ", upd)

			var contChanged []string
			var contDeleted []*ContainerInfo
			var appChanged []string
			var asgChanged []string

			if upd.Deleted {
				env.indexLock.Lock()
				c, ok := env.contIdx[upd.ContainerId]
				delete(env.contIdx, upd.ContainerId)
				if !ok {
					c.ContainerId = upd.ContainerId
				}
				contDeleted = append(contDeleted, &c)
				if c.AppId != "" {
					if app, ok := env.appIdx[c.AppId]; ok {
						delete(app.ContainerIps, c.ContainerId)
						env.appIdx[c.AppId] = app
						appChanged = append(appChanged, upd.AppId)
					}
				}
			} else {
				var spi *SpaceInfo
				var allsgs []cfclient.SecGroup
				var err error
				if upd.SpaceId != "" {
					spi, allsgs, err = env.fetchSpaceInfo(&upd.SpaceId)
					if err != nil {
						// TODO need to retry
					}
					// env.log.Debug(fmt.Sprintf("Space info: %+v", *spi))
				}
				var iseg *cfclient.IsolationSegment
				if spi != nil && spi.IsolationSegment != "" {
					iseg, err = env.ccClient.GetIsolationSegmentByGUID(spi.IsolationSegment)
					if err != nil {
						// TODO need to retry
					}
				}

				// update the indexes
				env.indexLock.Lock()

				// process spaces and ASGs
				for idx, _ := range allsgs {
					if updateAsg(&allsgs[idx]) {
						asgChanged = append(asgChanged, allsgs[idx].Guid)
					}
				}
				if spi != nil {
					env.spaceIdx[upd.SpaceId] = *spi
				}
				if iseg != nil {
					env.isoSegIdx[iseg.GUID] = IsoSegInfo{Id: iseg.GUID, Name: iseg.Name}
					// TODO: Check if name has changed, and if so update all containers in the isolation segment
				}

				// process Apps
				if upd.AppId != "" && upd.SpaceId != "" {
					app, ok := env.appIdx[upd.AppId]
					if !ok {
						app = AppInfo{AppId: upd.AppId, SpaceId: upd.SpaceId,
									  AppName: upd.AppName,
									  ContainerIps: make(map[string]string)}
					} else {
						app.SpaceId = upd.SpaceId
						if upd.AppName != "" {
							app.AppName = upd.AppName
						}
					}
					app.ContainerIps[upd.ContainerId] = upd.IpAddress
					env.appIdx[upd.AppId] = app
					appChanged = append(appChanged, upd.AppId)
				}

				c, ok := env.contIdx[upd.ContainerId]
				if !ok {
					env.contIdx[upd.ContainerId] = ContainerInfo{ContainerId: upd.ContainerId,
																AppId: upd.AppId,
																CellId: upd.CellId,
																IpAddress: upd.IpAddress,
																InstanceIndex: upd.InstanceIndex,
																Staging: upd.Staging,
																Ports: upd.Ports}
				} else {
					if upd.AppId != "" {
						c.AppId = upd.AppId
					}
					if upd.CellId != "" {
						c.CellId = upd.CellId
					}
					if upd.IpAddress != "" {
						c.IpAddress = upd.IpAddress
					}
					c.InstanceIndex = upd.InstanceIndex
					c.Ports = upd.Ports
					env.contIdx[upd.ContainerId] = c
				}
				contChanged = append(contChanged, upd.ContainerId)
			}

			for _, c := range contChanged {
				env.handleContainerUpdateLocked(c)
			}
			for _, c := range contDeleted {
				env.handleContainerDeleteLocked(c)
			}
			for _, a := range appChanged {
				env.handleAppUpdateLocked(a)
			}
			for _, g := range asgChanged {
				env.handleAsgUpdateLocked(g)
			}
			env.indexLock.Unlock()
		}
	}
	return
}

func (env *CfEnvironment) cleanupEtcdContainers() error {
	kapi := env.etcdKeysApi
	cellKey := etcd.CELL_KEY_BASE
	resp, err := kapi.Get(context.Background(), cellKey, &etcdclient.GetOptions{Recursive: true})
	if err != nil {
		env.log.Error("Unable to fetch etcd container nodes: ", err)
		return err
	}
	var nodes etcdclient.Nodes
	etcd.FlattenNodes(resp.Node, &nodes)

	env.indexLock.Lock()
	defer env.indexLock.Unlock()

	for _, n := range nodes {
		key_parts := strings.Split(n.Key, "/")
		// process keys of the form /aci/cells/<cell-id>/containers/<container-id>
		if len(key_parts) != 6 {
			continue
		}
		if key_parts[4] != "containers" {
			continue
		}
		cell_id := key_parts[3]
		cont_id := key_parts[5]
		c, ok := env.contIdx[cont_id]
		if !ok || c.CellId != cell_id {
			env.log.Info(fmt.Sprintf("Deleting stale container %s on cell %s", cont_id, cell_id))
			_, err := kapi.Delete(context.Background(), n.Key, &etcdclient.DeleteOptions{Recursive: true})
			if err != nil {
				env.log.Error("Error deleting container node: ", err)
			}
		}
	}
	return nil
}

func (env *CfEnvironment) initNetworkPolicyPoller(stopCh <-chan struct{}) {
	cont := env.cont
	var errDelayTime time.Duration = 10
	timer := time.NewTimer(1 * time.Second)
	oldRespHash := uint64(0)
	notifyChannel := true

	for {
		select {
		case <-stopCh:
			return

		case <-timer.C:
			allNetPol, err := env.netpolClient.GetPolicies()
			if err != nil {
				env.log.Error("Error fetching network policies: ", err)
				timer.Reset(errDelayTime * time.Second)
				continue
			}
			newRespHash, err := hashJsonSerializable(allNetPol)
			if err != nil {
				env.log.Warning("Failed to hash network policies response: " , err)
			} else if oldRespHash == newRespHash {
				timer.Reset(time.Duration(env.cfconfig.NetPolPollingInterval) *
							time.Second)
				continue
			}
			env.log.Debug(fmt.Sprintf("Fetched %d network-policies, oldHash %x newHash %x",
						  len(allNetPol), oldRespHash, newRespHash))
			// Diff against current net-policies
			npRead := make(map[string]map[string][]cfapi.Destination)
			for _, npol := range allNetPol {
				dst := npol.Destination.ID
				npol.Destination.ID = npol.Source.ID

				npDst, ok := npRead[dst]
				if !ok {
					npDst = make(map[string][]cfapi.Destination)
				}
				npDst[npol.Source.ID] = append(npDst[npol.Source.ID], npol.Destination)
				npRead[dst] = npDst
			}
			var updates []string
			var app_to_update []string
			to_delete := make(map[string]bool)
			env.indexLock.Lock()
			for k := range env.netpolIdx {
				to_delete[k] = true
			}
			// Update known net-policies
			for k, v := range npRead {
				oldnp, ok := env.netpolIdx[k]
				if !ok || !reflect.DeepEqual(oldnp, v) {
					env.netpolIdx[k] = v
					env.log.Debug(fmt.Sprintf("Add/update net pol %s: %+v", k, v))
					updates = append(updates, k)
					if !ok {
						app_to_update = append(app_to_update, k)
					}
				}
				to_delete[k] = false
			}
			// Process updated net-policies
			for _, polId := range updates {
				hpp := env.createHppForNetPol(&polId)
				cont.apicConn.WriteApicObjects("np:" + polId, hpp)
			}
			for k, v := range to_delete {
				if v {
					delete(env.netpolIdx, k)
					env.log.Debug("Delete net pol ", k)
					cont.apicConn.ClearApicObjects("np:" + k)
					app_to_update = append(app_to_update, k)
				}
			}
			for _, a := range app_to_update {
				env.updateAppContainersLocked(a)
			}
			env.indexLock.Unlock()
			oldRespHash = newRespHash
			if notifyChannel {
				env.idxStatusChan <- "net-policy-ready"
				notifyChannel = false
			}
			timer.Reset(time.Duration(env.cfconfig.NetPolPollingInterval) *
						time.Second)
		}
	}
}

func (env *CfEnvironment) createHppForNetPol(polId *string) apicapi.ApicSlice {
	// must be called with index lock

	npApicName := env.cont.aciNameForKey("np", *polId)
	hpp := apicapi.NewHostprotPol(env.cont.config.AciPolicyTenant, npApicName)
	for srcId, info := range env.netpolIdx[*polId] {
		ingressSubj := apicapi.NewHostprotSubj(hpp.GetDn(), "in-" + srcId)
		subjDn := ingressSubj.GetDn()
		for i, rule := range info {
			hpr := apicapi.NewHostprotRule(subjDn, fmt.Sprintf("rule_%d", i))
			hpr.SetAttr("direction", "ingress")
			hpr.SetAttr("ethertype", "ipv4")              // TODO fix for v6
			hpr.SetAttr("protocol", rule.Protocol)
			hpr.SetAttr("fromPort", fmt.Sprintf("%d", rule.Ports.Start))
			hpr.SetAttr("toPort", fmt.Sprintf("%d", rule.Ports.End))

			for _, ip := range env.appIdx[srcId].ContainerIps {
				hpremote := apicapi.NewHostprotRemoteIp(hpr.GetDn(), ip)
				hpr.AddChild(hpremote)
			}
			ingressSubj.AddChild(hpr)
		}
		hpp.AddChild(ingressSubj)
	}
	return apicapi.ApicSlice{hpp}
}