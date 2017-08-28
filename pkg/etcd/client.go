package etcd

import (
	"net"
	"net/http"
	"time"

	"code.cloudfoundry.org/cfhttp"
	"github.com/coreos/etcd/client"
)

func NewEtcdClient(etcdUrl string, caCertFile string, clientCertFile string,
				   clientKeyFile string) (client.Client, error) {
	tlsConfig, err := cfhttp.NewTLSConfig(clientCertFile, clientKeyFile, caCertFile)
	if err != nil {
		return nil, err
	}
	t := &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		Dial: (&net.Dialer{
			// values taken from http.DefaultTransport
			Timeout: 30 * time.Second,
			KeepAlive: 30 * time.Second,
		}).Dial,
		// value taken from http.DefaultTransport
		TLSHandshakeTimeout: 10 * time.Second,
		TLSClientConfig:     tlsConfig,
	}
	cfg := client.Config{
		Endpoints:               []string{etcdUrl},
		Transport:               t,
		// set timeout per request to fail fast when the target endpoint is unavailable
		HeaderTimeoutPerRequest: time.Second,
	}
	return client.New(cfg)
}

const (
	ACI_KEY_BASE = "/aci"
	CELL_KEY_BASE = "/aci/cells"
)

type GroupInfo struct {
	Tenant          string        `json:"group"`
	Group           string        `json:"tenant"`
}

type PortMap struct {
	ContainerPort   uint32         `json:"container_port"`
	HostPort        uint32         `json:"host_port"`
}

type EpInfo struct {
	AppId               string        `json:"app_id"`
	AppName             string        `json:"app_name"`
	IpAddress           string        `json:"ip_address"`
	InstanceIndex       int32         `json:"instance_index"`
	PortMapping         []PortMap     `json:"port_mapping"`
	EpgTenant           string        `json:"epg_tenant"`
	Epg                 string        `json:"epg"`
	SecurityGroups      []GroupInfo   `json:"sg"`
}

func FlattenNodes(nd *client.Node, nodes *client.Nodes) {
	if nd == nil {
		return
	}

	*nodes = append(*nodes, nd)
	for _, n := range nd.Nodes {
		FlattenNodes(n, nodes)
	}
}