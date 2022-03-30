package kv

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/fatih/color"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/server/v3/embed"

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

	notifier chan NewRole
	stop     chan struct{}

	leaderKey   string
	leaderValue string
}

func NewLeaderShip(etcd *embed.Etcd, client *clientv3.Client, key, value string) (*LeaderShip, error) {
	ret := &LeaderShip{
		etcd:        etcd,
		notifier:    make(chan NewRole, 128),
		client:      client,
		stop:        make(chan struct{}),
		lease:       &atomic.Value{},
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
	defer cancel()

	grantResp, err := ls.Grant(ctx, 10)
	if err != nil {
		log.Error("failed to grant lease")

		return false
	}
	l.leaseID = grantResp.ID

	resp, err := l.client.Txn(ctx).If(clientv3.Compare(clientv3.CreateRevision(l.leaderKey), "=", 0)).
		Then(clientv3.OpPut(l.leaderKey, l.leaderValue, clientv3.WithLease(grantResp.ID))).
		Commit()
	if err != nil || !resp.Succeeded {
		log.Debug("failed to compaign leader")
		return false
	}

	l.notifier <- NewRole{Role: BecameLeader}

	return true
}

func (l *LeaderShip) keepLeader() {
	ticker := time.NewTicker(time.Second * 5)
	defer ticker.Stop()

	for {
		ctx := context.TODO()
		_, err := l.client.KeepAliveOnce(ctx, l.leaseID)
		if err != nil {
			log.Info("failed to keep alive leadership lease")
			l.notifier <- NewRole{Role: BecameFollower}

			return
		}
		log.Info(color.GreenString("keep leader successfully."))
		select {
		case <-ticker.C:
		case <-l.stop:
			return
		}
	}
}

func (l *LeaderShip) Start() {
	go func() {
		ticker := time.NewTicker(time.Second * 5)
		defer ticker.Stop()
		for {
			success := l.Campaign()
			if success {
				l.keepLeader()
			}

			select {
			case <-ticker.C:
				continue
			case <-l.stop:
				return
			}
		}
	}()
}

func (l *LeaderShip) Stop() error {
	close(l.stop)
	return nil
}
