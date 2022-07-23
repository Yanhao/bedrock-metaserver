package scheduler

import (
	"errors"

	"sr.ht/moyanhao/bedrock-metaserver/common/log"
	"sr.ht/moyanhao/bedrock-metaserver/dataserver"
	"sr.ht/moyanhao/bedrock-metaserver/metadata"
)

func ClearDataserver(addr string) error {
	dm := metadata.GetDataServerManager()
	if !dm.IsDataServerExists(addr) {
		return metadata.ErrNoSuchDataServer
	}

	shardIDs, err := metadata.GetShardIDsInDataServer(addr)
	if err != nil {
		log.Error("GetShardsInDataServer failed, err: %v", err)
		return errors.New("GetShardsInDataServer failed")
	}

	sm := metadata.GetShardManager()
	conns := dataserver.GetDataServerConns()
	for _, shardID := range shardIDs {
		shard, err := sm.GetShardCopy(shardID)
		if err != nil {
			return err
		}

		var replicates []string
		for addr := range shard.Replicates {
			replicates = append(replicates, addr)
		}

		viableDataServers := generateViableDataServer(replicates)
		ds := randomSelect(viableDataServers)
		dataServerCli, _ := conns.GetApiClient(ds)

		err = dataServerCli.CreateShard(uint64(shardID), 0)
		if err != nil {
			log.Error("CreateShard failed, err: %v", err)
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
			sm.ReSelectLeader(shardID, metadata.WithLeader(shard.Leader))
		}

		dsTobeClearedCli, _ := conns.GetApiClient(addr)
		err = dsTobeClearedCli.DeleteShard(uint64(shardID), 0)
		if err != nil {
			log.Warn("DeleteShard failed, err: %v", err)
		}

		err = sm.RemoveShardReplicates(shardID, []string{addr})
		if err != nil {
			return err
		}
	}

	return nil
}
