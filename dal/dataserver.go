package dal

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"sr.ht/moyanhao/bedrock-metaserver/kv_engine"
	"sr.ht/moyanhao/bedrock-metaserver/model"
	"sr.ht/moyanhao/bedrock-metaserver/utils/log"
)

const (
	KvPrefixIdc             = "/idcs/"
	KvPrefixDataServer      = "/dataservers/"
	KvPrefixDataServerInIdc = "/dataservers_in_idc/"
)

func idcKey(idc string) string {
	return fmt.Sprintf("%s%s", KvPrefixIdc, strings.TrimSpace(idc))
}

func dataServerKey(addr string) string {
	return fmt.Sprintf("%s%s", KvPrefixDataServer, strings.TrimSpace(addr))
}

func dataServerInIdcKey(idc, addr string) string {
	return fmt.Sprintf("%s%s", dataServerInIdcPrefixKey(idc), addr)
}

func dataServerInIdcPrefixKey(idc string) string {
	return fmt.Sprintf("%s%s/", KvPrefixDataServerInIdc, idc)
}

func KvGetDataServer(addr string) (*model.DataServer, error) {
	ec := kv_engine.GetEtcdClient()
	resp, err := ec.KV.Get(context.Background(), dataServerKey(addr))
	if err != nil || resp.Count == 0 {
		return nil, ErrNoSuchShard
	}

	if resp.Count != 1 {
		return nil, errors.New("")
	}

	var dataserver model.DataServer
	for _, item := range resp.Kvs {
		err := dataserver.UnmarshalJSON(item.Value)
		if err != nil {
			return nil, err
		}
	}

	return &dataserver, nil
}

func KvPutDataServer(dataserver *model.DataServer) error {
	value, err := dataserver.MarshalJSON()
	if err != nil {
		log.Warn("failed to encode dataserver to pb, dataserver=%v", dataserver)
		return err
	}

	ec := kv_engine.GetEtcdClient()
	_, err = ec.Put(context.Background(), dataServerKey(dataserver.Addr()), string(value))
	if err != nil {
		log.Warn("failed to save dataserver to etcd, dataserver=%v", dataserver)
		return err
	}

	return nil
}

func KvDeleteDataServer(addr string) error {
	ec := kv_engine.GetEtcdClient()
	_, err := ec.Delete(context.Background(), dataServerKey(addr))
	if err != nil {
		log.Warn("failed to delete dataserver from kv")
		return err
	}

	return nil
}
