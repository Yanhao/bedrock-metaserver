package kvengine

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/fatih/color"
	clientv3 "go.etcd.io/etcd/client/v3"

	"sr.ht/moyanhao/bedrock-metaserver/config"
	"sr.ht/moyanhao/bedrock-metaserver/utils/log"
)

const (
	BecameLeader = iota
	BecameFollower
)

func IsMetaServerLeader() bool {
	return GetLeaderShip().IsLeader()
}

func GetMetaServerLeader() string {
	return GetLeaderShip().GetMetaServerLeader()
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

	leaderFunc   func()
	followerFunc func()
}

type LeaderShipOption struct {
	client *clientv3.Client

	key   string
	value string

	LeaderFunc   func()
	FollowerFunc func()
}

func sanitizeOptions(opts *LeaderShipOption) error {
	return nil
}

func NewLeaderShip(opts LeaderShipOption) (*LeaderShip, error) {
	if err := sanitizeOptions(&opts); err != nil {
		return nil, err
	}

	ret := &LeaderShip{
		notifier:    make(chan NewRole, 128),
		client:      opts.client,
		stop:        make(chan struct{}),
		lease:       &atomic.Value{},
		leaderKey:   opts.key,
		leaderValue: opts.value,
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

	return true
}

func (l *LeaderShip) LoadLeaderFromEtcd() error {
	resp, err := l.client.Get(context.TODO(), l.leaderKey)
	if err != nil {
		return err
	}
	if resp.Count == 0 {
		return errors.New("")
	}

	return nil
}

func (l *LeaderShip) IsLeader() bool {
	return GetMetaServerLeader() == l.leaderValue
}

func (l *LeaderShip) GetMetaServerLeader() string {
	return l.leaderValue
}

func (l *LeaderShip) keepLeader() {
	ticker := time.NewTicker(time.Second * 5)
	defer ticker.Stop()

	for {
		if l.LoadLeaderFromEtcd(); !l.IsLeader() {
			l.notifier <- NewRole{Role: BecameFollower}
			return
		}

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
			_ = l.LoadLeaderFromEtcd()

			select {
			case <-ticker.C:
				continue
			case <-l.stop:
				return
			}
		}
	}()

	go func() {
		for {
			leaderChangeNotifier := l.GetNotifier()

			select {
			case <-l.stop:
				log.Info("metaserver stopping")
				return
			case c := <-leaderChangeNotifier:
				switch c.Role {
				case BecameFollower:
					l.followerFunc()
				case BecameLeader:
					l.leaderFunc()
				}
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

var (
	leaderShip     *LeaderShip
	leaderShipOnce sync.Once
)

func GetLeaderShip() *LeaderShip {
	return leaderShip
}

func MustInitLeaderShip(client *clientv3.Client, leaderFunc, followerFunc func()) {
	opts := LeaderShipOption{
		client:       client,
		key:          "metaserver-leader",
		value:        config.GetConfiguration().ServerAddr,
		LeaderFunc:   leaderFunc,
		FollowerFunc: followerFunc,
	}

	l, err := NewLeaderShip(opts)
	if err != nil {
		panic(err)
	}

	l.Start()

	leaderShip = l
}
