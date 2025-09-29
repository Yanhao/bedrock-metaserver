package dal

import (
	"context"
	"errors"
	"fmt"
	"strings"

	log "github.com/sirupsen/logrus"
	client "go.etcd.io/etcd/client/v3"

	"sr.ht/moyanhao/bedrock-metaserver/meta_store"
	"sr.ht/moyanhao/bedrock-metaserver/model"
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
	resp, err := meta_store.GetEtcdClient().KV.Get(context.TODO(), dataServerKey(addr))
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
		log.Warnf("failed to encode dataserver to pb, dataserver=%v", dataserver)
		return err
	}

	_, err = meta_store.GetEtcdClient().Put(context.TODO(), dataServerKey(dataserver.Addr()), string(value))
	if err != nil {
		log.Warnf("failed to save dataserver to etcd, dataserver=%v", dataserver)
		return err
	}

	return nil
}

func KvDeleteDataServer(addr string) error {
	_, err := meta_store.GetEtcdClient().Delete(context.TODO(), dataServerKey(addr))
	if err != nil {
		log.Warn("failed to delete dataserver from kv")
		return err
	}

	return nil
}

func KvLoadAllDataServers() ([]*model.DataServer, error) {
	resp, err := meta_store.GetEtcdClient().Get(context.TODO(), KvPrefixDataServer, client.WithPrefix())
	if err != nil {
		log.Warn("failed to get dataserver from etcd")
		return nil, err
	}

	var ret []*model.DataServer
	for _, kv := range resp.Kvs {
		var dataserver model.DataServer
		if err := dataserver.UnmarshalJSON(kv.Value); err != nil {
			log.Warn("failed to decode dataserver from pb")
			return nil, err
		}

		ret = append(ret, &dataserver)
	}

	return ret, nil
}
