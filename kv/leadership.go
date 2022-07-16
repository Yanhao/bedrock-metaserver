package kv

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/fatih/color"
	clientv3 "go.etcd.io/etcd/client/v3"

	"sr.ht/moyanhao/bedrock-metaserver/common/log"
)

const (
	BecameLeader = iota
	BecameFollower
)

var (
	isMetaServerLeader int32
	metaServerLeader   atomic.Value
)

func IsMetaServerLeader() bool {
	return atomic.LoadInt32(&isMetaServerLeader) == 1
}

func GetMetaServerLeader() string {
	v := metaServerLeader.Load()
	if v == nil {
		return ""
	}

	return v.(string)
}

type NewRole struct {
	Role int
}

type LeaderShip struct {
	client  *clientv3.Client
	lease   *atomic.Value
	leaseID clientv3.LeaseID

	notifier chan NewRole
	stop     chan struct{}

	leaderKey   string
	leaderValue string
}

func NewLeaderShip(client *clientv3.Client, key, value string) (*LeaderShip, error) {
	ret := &LeaderShip{
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

	grantResp, err := ls.Grant(ctx, 10 /* 10 seconds */)
	if err != nil {
		log.Error("failed to grant lease")

		return false
	}
	l.leaseID = grantResp.ID

	resp, err := l.client.Txn(ctx).If(clientv3.Compare(clientv3.CreateRevision(l.leaderKey), "=", 0)).
		Then(clientv3.OpPut(l.leaderKey, l.leaderValue, clientv3.WithLease(grantResp.ID))).
		Commit()
	if err != nil || !resp.Succeeded {
		log.Debug("failed to campaign leader")
		ls.Close()
		return false
	}

	l.notifier <- NewRole{Role: BecameLeader}
	atomic.StoreInt32(&isMetaServerLeader, 1)

	return true
}

func (l *LeaderShip) IsLeader() bool {
	resp, err := l.client.Get(context.TODO(), l.leaderKey)
	if err != nil {
		return false
	}
	if resp.Count == 0 {
		return false
	}
	metaServerLeader.Store(string(resp.Kvs[0].Value))

	return string(resp.Kvs[0].Value) == l.leaderValue
}

func (l *LeaderShip) keepLeader() {
	ticker := time.NewTicker(time.Second * 5)
	defer ticker.Stop()

	for {
		if !l.IsLeader() {
			l.notifier <- NewRole{Role: BecameFollower}
			atomic.StoreInt32(&isMetaServerLeader, 0)
			return
		}

		ctx := context.TODO()
		_, err := l.client.KeepAliveOnce(ctx, l.leaseID)
		if err != nil {
			log.Info("failed to keep alive leadership lease")
			l.notifier <- NewRole{Role: BecameFollower}
			atomic.StoreInt32(&isMetaServerLeader, 0)

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

func (l *LeaderShip) Reset() {
}
