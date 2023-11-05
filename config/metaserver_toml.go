package config

import (
	"errors"
	"os"

	"github.com/BurntSushi/toml"
)

type EtcdConfig struct {
	Name             string `toml:"name"`
	DataDir          string `toml:"data_dir"`
	WalDir           string `toml:"wal_dir"`
	PreVote          bool   `toml:"pre_vote"`
	PeerAddr         string `toml:"peer_addr"`
	ClientAddr       string `toml:"client_addr"`
	ClientTimeoutSec int64  `toml:"client_timeout_sec"`
	ClusterPeers     string `toml:"cluster_peers"`
}

type SchedulerConfig struct {
	BigShardSizeThreshold           int64 `toml:"big_shard_size_threshold"`
	DataserverSpaceBalanceThreshold int64 `toml:"dataserver_space_balance_threshold"`
}
type ServerConfig struct {
	Addr                   string `toml:"addr"`
	PprofListenAddr        string `toml:"pprof_listen_addr"`
	LogFile                string `toml:"log_file"`
	EnableHeartBeatChecker bool   `toml:"enable_heart_beat_checker"`
}

type Configuration struct {
	Etcd      EtcdConfig      `toml:"etcd"`
	Scheduler SchedulerConfig `toml:"scheduler"`
	Server    ServerConfig    `toml:"server"`
}

var msConfig *Configuration

func GetConfig() *Configuration {
	return msConfig
}

func loadConfigFromFile(filePath string) error {
	f, err := os.Open(filePath)
	if err != nil {
		return errors.New("failed to open configuration file")
	}
	defer f.Close()

	buf := make([]byte, 1024*8)
	count, err := f.Read(buf)
	if err != nil {
		return errors.New("failed to read configuration file")
	}
	_ = f.Close()

	var c Configuration
	if err := toml.Unmarshal(buf[:count], &c); err != nil {
		return errors.New("failed to parse toml file")
	}

	msConfig = &c
	return nil
}

// just panic if there are something wrong
func validateConfig() {

}

func MustLoadConfig(configFile string) {
	if err := loadConfigFromFile(configFile); err != nil {
		panic("failed to load configuration file")
	}

	validateConfig()
}
