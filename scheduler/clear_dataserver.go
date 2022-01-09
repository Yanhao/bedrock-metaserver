package scheduler

import (
	"errors"

	"sr.ht/moyanhao/bedrock-metaserver/dataserver"
	"sr.ht/moyanhao/bedrock-metaserver/metadata"
)

func ClearDataserver(addr string) error {
	if !metadata.IsDataServerActive(addr) {
		return metadata.ErrNoSuchDataServer
	}

	shardIDs, err := metadata.GetShardsInDataServer(addr)
	if err != nil {
		return errors.New("")
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
			return err
		}

		err = dataServerCli.RepairShard(uint64(shardID), shard.Leader)
		if err != nil {
			return err
		}

		shard.AddReplicates([]string{ds})

		if shard.Leader == addr {
			sm.ReSelectLeader(shardID)
		}

		dsTobeClearedCli := conns.GetApiClient(addr)
		err = dsTobeClearedCli.DeleteShard(uint64(shardID))
		if err != nil {
			return err
		}

		shard.RemoveReplicates([]string{addr})
		err = sm.PutShard(shard)
		if err != nil {
			return err
		}
	}

	return nil
}
