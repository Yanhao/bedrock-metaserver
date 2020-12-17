package kv

import (
	"context"
	"sync/atomic"
	"time"

	"go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/embed"

	"sr.ht/moyanhao/bedrock-metaserver/common/log"
)

const (
	BecameLeader = iota
	BecameFollower
)

type NewRole struct {
	Role int
}

type LeaderShip struct {
	etcd    *embed.Etcd
	client  *clientv3.Client
	lease   *atomic.Value
	leaseID clientv3.LeaseID

	notifier    chan NewRole
	stop        chan struct{}
	leaderKey   string
	leaderValue string
}

func NewLeaderShip(etcd *embed.Etcd, client *clientv3.Client, key, value string) (*LeaderShip, error) {
	ret := &LeaderShip{
		etcd:        etcd,
		notifier:    make(chan NewRole, 128),
		client:      client,
		stop:        make(chan struct{}),
		lease:       nil,
		leaderKey:   key,
		leaderValue: value,
	}

	return ret, nil
}

func (l *LeaderShip) setLease(lease *clientv3.Lease) {
	l.lease.Store(lease)
}

func (l *LeaderShip) getLease() *clientv3.Lease {
	ls := l.lease.Load()
	if ls == nil {
		return nil
	}
	return ls.(*clientv3.Lease)
}

func (l *LeaderShip) GetNotifier() <-chan NewRole {
	return l.notifier
}

func (l *LeaderShip) Campaign() bool {
	ls := clientv3.NewLease(l.client)
	l.setLease(&ls)

	ctx, cancel := context.WithTimeout(l.client.Ctx(), 3*time.Second)
	grantResp, err := ls.Grant(ctx, 10)
	if err != nil {
		log.Error("failed to grant lease")
		return false
	}
	cancel()
	l.leaseID = grantResp.ID

	resp, err := l.client.Txn(ctx).If(clientv3.Compare(clientv3.CreateRevision(l.leaderKey), "=", 0)).
		Then(clientv3.OpPut(l.leaderKey, l.leaderValue, clientv3.WithLease(grantResp.ID))).
		Commit()
	if err != nil || !resp.Succeeded {
		log.Error("failed to compaign leader")
		return false
	}

	l.notifier <- NewRole{Role: BecameLeader}
	return true
}

func (l *LeaderShip) keepLeader() {
	tiker := time.NewTicker(time.Second)
	defer tiker.Stop()

	for {
		ctx := context.TODO()
		_, err := l.client.KeepAliveOnce(ctx, l.leaseID)
		if err != nil {
			log.Info("failed to keep alive leadership lease")
			l.notifier <- NewRole{Role: BecameFollower}
			return
		}
		select {
		case <-tiker.C:
		case <-l.stop:
			return
		}
	}
}

func (l *LeaderShip) Start() {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for {
		success := l.Campaign()
		if success {
			l.keepLeader()
		}

		ticker.Reset(time.Second)
		select {
		case <-ticker.C:
			continue
		case <-l.stop:
			return
		}
	}
}

func (l *LeaderShip) Stop() error {
	close(l.stop)
	return nil
}

func (l *LeaderShip) LeaderTxn(cs ...clientv3.Cmp) (error, clientv3.Txn) {
	ctx, _ := context.WithTimeout(l.client.Ctx(), 10*time.Second)
	txn := l.client.Txn(ctx)
	return nil, txn
}
