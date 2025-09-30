package dal

import (
	"context"
	"fmt"
	"strings"

	log "github.com/sirupsen/logrus"
	client "go.etcd.io/etcd/client/v3"

	"sr.ht/moyanhao/bedrock-metaserver/errors"
	"sr.ht/moyanhao/bedrock-metaserver/meta_store"
	"sr.ht/moyanhao/bedrock-metaserver/model"
)

var (
	ErrNoSuchDataServer = errors.ErrNoSuchDataServer
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
	etcdClient, err := meta_store.GetEtcdClient()
	if err != nil {
		return nil, errors.Wrap(err, errors.ErrCodeSystem, "failed to get etcd client")
	}
	resp, err := etcdClient.KV.Get(context.TODO(), dataServerKey(addr))
	if err != nil {
		log.Errorf("failed to get data server from etcd, addr=%s, err=%v", addr, err)
		return nil, errors.Wrap(err, errors.ErrCodeDatabase, "failed to get data server")
	}
	if resp.Count == 0 {
		return nil, ErrNoSuchDataServer
	}

	if resp.Count != 1 {
		log.Errorf("multiple data server entries found for addr=%s, count=%d", addr, resp.Count)
		return nil, errors.New(errors.ErrCodeDatabase, "multiple data server entries found")
	}

	var dataserver model.DataServer
	for _, item := range resp.Kvs {
		err := dataserver.UnmarshalJSON(item.Value)
		if err != nil {
			log.Errorf("failed to unmarshal data server, addr=%s, err=%v", addr, err)
			return nil, errors.Wrap(err, errors.ErrCodeInternal, "failed to unmarshal data server")
		}
	}

	return &dataserver, nil
}

func KvPutDataServer(dataserver *model.DataServer) error {
	value, err := dataserver.MarshalJSON()
	if err != nil {
		log.Warnf("failed to encode dataserver to pb, dataserver=%v", dataserver)
		return errors.Wrap(err, errors.ErrCodeInternal, "failed to encode data server")
	}

	etcdClient, err := meta_store.GetEtcdClient()
	if err != nil {
		return errors.Wrap(err, errors.ErrCodeSystem, "failed to get etcd client")
	}
	_, err = etcdClient.Put(context.TODO(), dataServerKey(dataserver.Addr()), string(value))
	if err != nil {
		log.Warnf("failed to save dataserver to etcd, dataserver=%v", dataserver)
		return errors.Wrap(err, errors.ErrCodeDatabase, "failed to save data server")
	}

	return nil
}

func KvDeleteDataServer(addr string) error {
	etcdClient, err := meta_store.GetEtcdClient()
	if err != nil {
		return errors.Wrap(err, errors.ErrCodeSystem, "failed to get etcd client")
	}
	_, err = etcdClient.Delete(context.TODO(), dataServerKey(addr))
	if err != nil {
		log.Warn("failed to delete dataserver from kv")
		return errors.Wrap(err, errors.ErrCodeDatabase, "failed to delete data server")
	}

	return nil
}

func KvLoadAllDataServers() ([]*model.DataServer, error) {
	etcdClient, err := meta_store.GetEtcdClient()
	if err != nil {
		return nil, errors.Wrap(err, errors.ErrCodeSystem, "failed to get etcd client")
	}
	resp, err := etcdClient.Get(context.TODO(), KvPrefixDataServer, client.WithPrefix())
	if err != nil {
		log.Warn("failed to get dataserver from etcd")
		return nil, errors.Wrap(err, errors.ErrCodeDatabase, "failed to get data servers")
	}

	var ret []*model.DataServer
	for _, kv := range resp.Kvs {
		var dataserver model.DataServer
		if err := dataserver.UnmarshalJSON(kv.Value); err != nil {
			log.Warn("failed to decode dataserver from pb")
			return nil, errors.Wrap(err, errors.ErrCodeInternal, "failed to decode data server")
		}

		ret = append(ret, &dataserver)
	}

	return ret, nil
}
