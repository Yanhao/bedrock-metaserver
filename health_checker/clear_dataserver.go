package health_checker

import (
	"errors"
	"math/rand"

	log "github.com/sirupsen/logrus"

	"sr.ht/moyanhao/bedrock-metaserver/clients/dataserver"
	"sr.ht/moyanhao/bedrock-metaserver/manager"
)

func ClearDataserver(addr string) error {
	dm := manager.GetDataServerManager()
	if !dm.IsDataServerExists(addr) {
		return manager.ErrNoSuchDataServer
	}
	sm := manager.GetShardManager()
	shardIDs, err := sm.GetShardIDsInDataServer(addr)
	if err != nil {
		log.Errorf("GetShardsInDataServer failed, err: %v", err)
		return errors.New("GetShardsInDataServer failed")
	}

	conns := dataserver.GetDataServerConns()
	for _, shardID := range shardIDs {
		shard, err := sm.GetShard(shardID)
		if err != nil {
			return err
		}

		var replicates []string
		for addr := range shard.Replicates {
			replicates = append(replicates, addr)
		}

		viableDataServers := manager.GetDataServerManager().GenerateViableDataServer(replicates)
		if len(viableDataServers) == 0 {
			continue
		}

		ds := viableDataServers[rand.Intn(len(viableDataServers))]
		dataServerCli, _ := conns.GetApiClient(ds)

		err = dataServerCli.CreateShard(uint64(shardID), shard.RangeKeyStart, shard.RangeKeyEnd)
		if err != nil {
			log.Errorf("CreateShard failed, err: %v", err)
			return err
		}

		err = sm.AddShardReplicates(shardID, []string{ds})
		if err != nil {
			return err
		}

		if shard.Leader == addr {
			sm.ReSelectLeader(shardID)
		} else {
			// notify leader the shard member change
			sm.ReSelectLeader(shardID, manager.WithLeader(shard.Leader))
		}

		dsTobeClearedCli, _ := conns.GetApiClient(addr)
		err = dsTobeClearedCli.DeleteShard(uint64(shardID))
		if err != nil {
			log.Warnf("DeleteShard failed, err: %v", err)
		}

		err = sm.RemoveShardReplicates(shardID, []string{addr})
		if err != nil {
			return err
		}
	}

	return nil
}
