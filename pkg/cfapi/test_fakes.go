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

//
// This file defines fake versions of various client that are meant to be used
// for testing ONLY.
//

package cfapi

import (
	cfclient "github.com/cloudfoundry-community/go-cfclient"
)

type FakeCcClient struct {
	err                     error

	GetSpaceByGuid_response cfclient.Space
	ListSecGroupsBySpace_response []cfclient.SecGroup
	GetIsolationSegmentByGUID_response *cfclient.IsolationSegment
	GetOrgDefaultIsolationSegment_response string
	GetSpaceIsolationSegment_response string
	GetUserRoleInfo_response *UserRoleInfo
	GetAppSpace_response string
}

func (c *FakeCcClient) GetSpaceByGuid(spaceGUID string) (cfclient.Space, error) {
	return c.GetSpaceByGuid_response, c.err
}

func (c *FakeCcClient) ListSecGroupsBySpace(spaceGuid string, staging bool) ([]cfclient.SecGroup, error) {
	return c.ListSecGroupsBySpace_response, c.err
}

func (c *FakeCcClient) GetIsolationSegmentByGUID(guid string) (*cfclient.IsolationSegment, error) {
	return c.GetIsolationSegmentByGUID_response, c.err
}

func (c *FakeCcClient) GetOrgDefaultIsolationSegment(orgGuid string) (string, error) {
	return c.GetOrgDefaultIsolationSegment_response, c.err
}

func (c *FakeCcClient) GetSpaceIsolationSegment(spaceGuid string) (string, error) {
	return c.GetSpaceIsolationSegment_response, c.err
}

func (c *FakeCcClient) GetUserRoleInfo(userGuid string) (*UserRoleInfo, error) {
	return c.GetUserRoleInfo_response, c.err
}

func (c *FakeCcClient) GetAppSpace(appGuid string) (string, error) {
	return c.GetAppSpace_response, c.err
}

type FakeCfAuthClient struct {
	err                      error
	FetchTokenInfo_response  *TokenInfo
}

func (c *FakeCfAuthClient) FetchTokenInfo(token string) (*TokenInfo, error) {
	return c.FetchTokenInfo_response, c.err
}
