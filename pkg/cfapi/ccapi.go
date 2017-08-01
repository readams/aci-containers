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

package cfapi

import (
	"encoding/json"
	"io/ioutil"

	"github.com/Sirupsen/logrus"
	
	cfclient "github.com/cloudfoundry-community/go-cfclient"
)

type CcClient struct {
	cfclient.Client
	log *logrus.Logger
}

func NewCcClient(apiUrl string, username string, password string, log *logrus.Logger) (*CcClient, error) {
	ccConfig := &cfclient.Config{
		ApiAddress: apiUrl,
		Username: username,
		Password: password,
		SkipSslValidation: true,
	}
	cfc, err := cfclient.NewClient(ccConfig)
	if err != nil {
		return nil, err
	}
	return &CcClient{*cfc, log}, nil
}

func (ccClient *CcClient) ListSecGroupsBySpace(spaceGuid string, staging bool) ([]cfclient.SecGroup, error) {
	stageStr := ""
	if staging {
		stageStr = "staging_"
	}
	requestURL := "/v2/spaces/" + spaceGuid + "/" + stageStr + "security_groups"
	var secGroups []cfclient.SecGroup
	for requestURL != "" {
		var secGroupResp cfclient.SecGroupResponse
		r := ccClient.NewRequest("GET", requestURL)
		resp, err := ccClient.DoRequest(r)

		if err != nil {
			return nil, err
		}
		resBody, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return nil, err
		}

		err = json.Unmarshal(resBody, &secGroupResp)
		if err != nil {
			return nil, err
		}

		for _, secGroup := range secGroupResp.Resources {
			secGroup.Entity.Guid = secGroup.Meta.Guid
			secGroups = append(secGroups, secGroup.Entity)
		}

		requestURL = secGroupResp.NextUrl
		resp.Body.Close()
	}
	return secGroups, nil
}

func (ccClient *CcClient) GetAppSummary(appGuid string) (string, error) {
	requestURL := "/v2/apps/" + appGuid + "/summary"
	
	r := ccClient.NewRequest("GET", requestURL)
	resp, err := ccClient.DoRequest(r)

	if err != nil {
		return "", err
	}
	resBody, err := ioutil.ReadAll(resp.Body)	
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	return string(resBody), nil
}

type AppUsageEventResponse struct {
	Results   int                `json:"total_results"`
	Pages     int                `json:"total_pages"`
	PrevURL   string             `json:"prev_url"`
	NextURL   string             `json:"next_url"`
	Resources []AppUsageEventResource `json:"resources"`
}

type AppUsageEventResource struct {
	Meta   cfclient.Meta           `json:"metadata"`
	Entity AppUsageEventEntity     `json:"entity"`
}

type AppUsageEventEntity struct {
	Guid  string
	State string           `json:"state"`
	AppGuid string         `json:"app_guid"`
	ParentAppGuid string   `json:"parent_app_guid"`
	AppName string         `json:"app_name"`
	SpaceGuid string       `json:"space_guid"`
	SpaceName string       `json:"space_name"`
	OrgGuid string         `json:"org_guid"`
}

func (ccClient *CcClient) GetAppUsageEvents(afterGuid string) ([]AppUsageEventEntity, error) {
	requestURL := "/v2/app_usage_events"
	if afterGuid != "" {
		requestURL = requestURL + "?after_guid=" + afterGuid
	}
	
	var events []AppUsageEventEntity
	for requestURL != "" {
		ccClient.log.Debug("Invoking GET ", requestURL)
		r := ccClient.NewRequest("GET", requestURL)
		resp, err := ccClient.DoRequest(r)
	
		if err != nil {
			return nil, err
		}
		resBody, err := ioutil.ReadAll(resp.Body)	
		if err != nil {
			return nil, err
		}
		
		var respDecoded AppUsageEventResponse
		err = json.Unmarshal(resBody, &respDecoded)
		if err != nil {
			return nil, err
		}
		for _, er := range respDecoded.Resources {
			er.Entity.Guid = er.Meta.Guid
			events = append(events, er.Entity)
		}
		requestURL = respDecoded.NextURL
		resp.Body.Close()
	}
	return events, nil
}
