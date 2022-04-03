package scheduler

import (
	"errors"

	"sr.ht/moyanhao/bedrock-metaserver/common/log"
	"sr.ht/moyanhao/bedrock-metaserver/dataserver"
	"sr.ht/moyanhao/bedrock-metaserver/metadata"
)

func ClearDataserver(addr string) error {
	if !metadata.IsDataServerExists(addr) {
		return metadata.ErrNoSuchDataServer
	}

	shardIDs, err := metadata.GetShardsInDataServerInKv(addr)
	if err != nil {
		log.Error("GetShardsInDataServer failed, err: %v", err)
		return errors.New("GetShardsInDataServer failed")
	}

	sm := metadata.GetShardManager()
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

		viableDataServers := generateViableDataServer(replicates)
		ds := randomSelect(viableDataServers)
		dataServerCli := conns.GetApiClient(ds)

		err = dataServerCli.CreateShard(uint64(shardID))
		if err != nil {
			log.Error("CreateShard failed, err: %v", err)
			return err
		}

		err = dataServerCli.RepairShard(uint64(shardID), shard.Leader)
		if err != nil {
			log.Error("RepairShard failed, err: %v", err)
			return err
		}
		log.Info("repaired shard %v in %v", shardID, addr)

		shard.AddReplicates([]string{ds})

		if shard.Leader == addr {
			shard.ReSelectLeader()
		}

		dsTobeClearedCli := conns.GetApiClient(addr)
		err = dsTobeClearedCli.DeleteShard(uint64(shardID))
		if err != nil {
			log.Error("DeleteShard failed, err: %v", err)
			return err
		}

		shard.RemoveReplicates([]string{addr})
		err = sm.PutShard(shard)
		if err != nil {
			log.Error("PutShard failed, err: %v", err)
			return err
		}
	}

	return nil
}
