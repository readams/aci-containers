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
	"flag"

	"github.com/noironetworks/aci-containers/pkg/ipam"
)

type opflexGroup struct {
	PolicySpace string `json:"policy-space,omitempty"`
	Name        string `json:"name,omitempty"`
}

// Configuration for the controller
type ControllerConfig struct {
	// Log level
	LogLevel string `json:"log-level,omitempty"`

	// Absolute path to a kubeconfig file
	KubeConfig string `json:"kubeconfig,omitempty"`

	// TCP port to run status server on (or 0 to disable)
	StatusPort int `json:"status-port,omitempty"`

	// Default endpoint group annotation value
	DefaultEg opflexGroup `json:"default-endpoint-group,omitempty"`

	// Default security group annotation value
	DefaultSg []opflexGroup `json:"default-security-group,omitempty"`

	// Name prefix to use when creating policy to avoid namespace
	// collisions
	AciPrefix string `json:"aci-prefix,omitempty"`

	// Tenant to use when creating policy objects in APIC
	AciPolicyTenant string `json:"aci-policy-tenant,omitempty"`

	// Physical domain used for service device clusters
	AciServicePhysDom string `json:"aci-service-phys-dom,omitempty"`

	// Encap used for service device clusters
	AciServiceEncap string `json:"aci-service-encap,omitempty"`

	// Tenant containing L3 out to use for services, service device
	// clusters need to be created in this tenant
	AciL3OutTenant string `json:"aci-l3out-tenant,omitempty"`

	// L3 out to use for services
	AciL3Out string `json:"aci-l3out,omitempty"`

	// ACI VRF for this kubernetes instance
	AciVrf string `json:"aci-vrf,omitempty"`

	// L3 external networks (within the l3out) that will be able to
	// access the service IPs
	AciExtNetworks []string `json:"aci-ext-networks,omitempty"`

	// IP addresses used for pod network
	PodIpPool []ipam.IpRange `json:"pod-ip-pool,omitempty"`

	// The number of IP addresses to allocate when a pod starts to run low
	PodIpPoolChunkSize int `json:"pod-ip-pool-chunk-size,omitempty"`

	// IP addresses used for externally exposed load balanced services
	ServiceIpPool []ipam.IpRange `json:"service-ip-pool,omitempty"`

	// IP addresses that can be requested as static service IPs in
	// service spec
	StaticServiceIpPool []ipam.IpRange `json:"static-service-ip-pool,omitempty"`

	// IP addresses to use for node service endpoints
	NodeServiceIpPool []ipam.IpRange `json:"node-service-ip-pool,omitempty"`

	// a list of subnet/gateway CIDR addresses that cover the
	// addresses in the node service IP pool
	NodeServiceSubnets []string `json:"node-service-subnets,omitempty"`
}

type netIps struct {
	V4 *ipam.IpAlloc
	V6 *ipam.IpAlloc
}

func newNetIps() *netIps {
	return &netIps{
		V4: ipam.New(),
		V6: ipam.New(),
	}
}

func NewConfig() *ControllerConfig {
	return &ControllerConfig{
		DefaultSg:          make([]opflexGroup, 0),
		AciPolicyTenant:    "kubernetes",
		PodIpPoolChunkSize: 128,
	}
}

func InitFlags(config *ControllerConfig) {
	flag.StringVar(&config.LogLevel, "log-level", "info", "Log level")

	flag.StringVar(&config.KubeConfig, "kubeconfig", "", "Absolute path to a kubeconfig file")

	flag.IntVar(&config.StatusPort, "status-port", 8091, " TCP port to run status server on (or 0 to disable)")
}

func (cont *AciController) loadIpRanges(v4 *ipam.IpAlloc, v6 *ipam.IpAlloc,
	ipranges []ipam.IpRange) {

	for _, r := range ipranges {
		if r.Start.To4() != nil && r.End.To4() != nil {
			v4.AddRange(r.Start, r.End)
		} else if r.Start.To16() != nil && r.End.To16() != nil {
			v6.AddRange(r.Start, r.End)
		} else {
			cont.log.Warn("Range invalid: ", r)
		}
	}
}

func (cont *AciController) initIpam() {
	cont.loadIpRanges(cont.configuredPodNetworkIps.V4, cont.configuredPodNetworkIps.V6,
		cont.config.PodIpPool)
	cont.podNetworkIps.V4.AddAll(cont.configuredPodNetworkIps.V4)
	cont.podNetworkIps.V6.AddAll(cont.configuredPodNetworkIps.V6)
	cont.loadIpRanges(cont.serviceIps.V4, cont.serviceIps.V6, cont.config.ServiceIpPool)
	cont.loadIpRanges(cont.staticServiceIps.V4, cont.staticServiceIps.V6,
		cont.config.StaticServiceIpPool)
	cont.loadIpRanges(cont.nodeServiceIps.V4, cont.nodeServiceIps.V6,
		cont.config.NodeServiceIpPool)
}