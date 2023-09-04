package config

import (
	"errors"
	"net/url"
	"os"
	"time"

	"github.com/pelletier/go-toml"
)

type Configuration struct {
	EtcdName          string
	EtcdDataDir       string
	EtcdWalDir        string
	EtcdPreVote       bool
	EtcdPeerAddr      *url.URL
	EtcdClientAddr    *url.URL
	EtcdClientTimeout time.Duration
	EtcdClusterPeers  string

	ServerAddr             string
	PprofListenAddr        string
	LogFile                string
	EnableHeartBeatChecker bool
}

var MsConfig *Configuration

func GetConfiguration() *Configuration {
	return MsConfig
}

func loadConfigFromFile(filePath string) (*Configuration, error) {
	ret := &Configuration{}

	f, err := os.Open(filePath)
	if err != nil {
		return ret, errors.New("failed to open configuration file")
	}
	defer f.Close()

	buf := make([]byte, 1024*8)

	count, err := f.Read(buf)
	if err != nil {
		return ret, errors.New("failed to read configuration file")
	}
	_ = f.Close()

	c, err := toml.Load(string(buf[:count]))
	if err != nil {
		return ret, errors.New("failed to parse toml file")
	}

	ret.EtcdName = c.Get("etcd.name").(string)
	ret.EtcdDataDir = c.Get("etcd.data_dir").(string)
	ret.EtcdWalDir = c.Get("etcd.wal_dir").(string)
	ret.EtcdPreVote = c.Get("etcd.prevote").(bool)

	etcdPeerAddrStr := c.Get("etcd.raft_addr").(string)
	ret.EtcdPeerAddr, _ = url.Parse(etcdPeerAddrStr)

	etcdClientAddrStr := c.Get("etcd.client_addr").(string)
	ret.EtcdClientAddr, _ = url.Parse(etcdClientAddrStr)

	ret.EtcdClusterPeers = c.Get("etcd.peers").(string)
	ret.EtcdClientTimeout = time.Duration(c.Get("etcd.client_timeout").(int64)) * time.Second

	ret.LogFile = c.Get("server.log_file").(string)
	ret.ServerAddr = c.Get("server.addr").(string)
	ret.PprofListenAddr = c.Get("server.pprof_addr").(string)
	ret.EnableHeartBeatChecker = c.Get("server.enable_heartbeat_checker").(bool)

	MsConfig = ret
	return MsConfig, nil
}

// just panic if there are something wrong
func validateConfig() {

}

func MustLoadConfig(configFile string) {
	if _, err := loadConfigFromFile(configFile); err != nil {
		panic("failed to load configuration file")
	}

	validateConfig()
}
