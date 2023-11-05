package role

import (
	"context"
	"errors"
	"fmt"
	"runtime/debug"
	"sync/atomic"
	"time"

	"github.com/fatih/color"
	log "github.com/sirupsen/logrus"
	clientv3 "go.etcd.io/etcd/client/v3"

	"sr.ht/moyanhao/bedrock-metaserver/config"
)

const (
	BecameLeader = iota
	BecameFollower
)

type NewRole struct {
	Role int
}

type LeaderShip struct {
	client  *clientv3.Client
	lease   *atomic.Value
	leaseID clientv3.LeaseID

	notifier chan NewRole
	stop     chan struct{}

	currentLeader atomic.Value

	leaderKey   string
	leaderValue string

	leaseTime   int // in second
	renewalTime int // in second

	leaderFunc   func()
	followerFunc func()
}

type LeaderShipOption struct {
	client *clientv3.Client

	key   string
	value string

	leaseTime   int // in second
	renewalTime int // in second

	LeaderFunc   func()
	FollowerFunc func()
}

func sanitizeOptions(opts *LeaderShipOption) error {
	if opts.client == nil {
		return errors.New("invalid etcd client")
	}

	if opts.LeaderFunc == nil || opts.FollowerFunc == nil {
		return errors.New("empty leader func or followr func")
	}

	if opts.key == "" || opts.value == "" {
		return errors.New("empty key or value")
	}

	return nil
}

func NewLeaderShip(opts LeaderShipOption) (*LeaderShip, error) {
	if err := sanitizeOptions(&opts); err != nil {
		return nil, err
	}

	ret := &LeaderShip{
		notifier:     make(chan NewRole, 128),
		client:       opts.client,
		stop:         make(chan struct{}),
		lease:        &atomic.Value{},
		leaseTime:    opts.leaseTime,
		renewalTime:  opts.renewalTime,
		leaderKey:    opts.key,
		leaderValue:  opts.value,
		leaderFunc:   opts.LeaderFunc,
		followerFunc: opts.FollowerFunc,
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

func (l *LeaderShip) campaign() bool {
	// creat a new lease with 10s, record it within LeaderShip
	ls := clientv3.NewLease(l.client)
	l.setLease(&ls)

	ctx, cancel := context.WithTimeout(l.client.Ctx(), 5*time.Second)
	defer cancel()

	grantResp, err := ls.Grant(ctx, int64(l.leaseTime) /* 10 seconds by default */)
	if err != nil {
		log.Error("failed to grant lease")

		return false
	}
	l.leaseID = grantResp.ID

	// try to compaign
	resp, err := l.client.Txn(ctx).
		If(clientv3.Compare(clientv3.CreateRevision(l.leaderKey), "=", 0)).
		Then(clientv3.OpPut(l.leaderKey, l.leaderValue, clientv3.WithLease(grantResp.ID))).
		Commit()
	if err != nil || !resp.Succeeded {
		log.Debug("failed to campaign leader")
		_ = ls.Close()

		return false
	}

	l.notifier <- NewRole{Role: BecameLeader}

	return true
}

func (l *LeaderShip) keepLeader() {
	ticker := time.NewTicker(time.Duration(l.renewalTime) * time.Second)
	defer ticker.Stop()

	for {
		l.currentLeader.Store(l.leaderValue)

		select {
		case <-ticker.C:
		case <-l.stop:
			return
		}

		// in case of the leader key was delete by accident but the lease still there
		if l.loadLeaderFromKv() == nil && !l.IsMetaServerLeader() {
			l.notifier <- NewRole{Role: BecameFollower}
			return
		}

		_, err := l.client.KeepAliveOnce(context.TODO(), l.leaseID)
		if err != nil {
			log.Info("failed to keep alive leadership lease")
			l.notifier <- NewRole{Role: BecameFollower}

			return
		}

		log.Info(color.GreenString("keep leader successfully."))
	}
}

func (l *LeaderShip) loadLeaderFromKv() error {
	resp, err := l.client.Get(context.TODO(), l.leaderKey)
	if err != nil {
		return err
	}

	if resp.Count == 0 {
		return errors.New("")
	}

	l.currentLeader.Store(string(resp.Kvs[0].Value))

	return nil
}

func (l *LeaderShip) GetNotifier() <-chan NewRole {
	return l.notifier
}

func (l *LeaderShip) IsMetaServerLeader() bool {
	return l.GetMetaServerLeader() == l.leaderValue
}

func (l *LeaderShip) GetMetaServerLeader() string {
	return l.currentLeader.Load().(string)
}

func (l *LeaderShip) Start() {
	go func() {
		ticker := time.NewTicker(time.Duration(l.renewalTime) * time.Second)
		defer ticker.Stop()

		for {
			success := l.campaign()
			if success {
				l.keepLeader()
			} else {
				_ = l.loadLeaderFromKv()
			}

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

var (
	leaderShip *LeaderShip
)

func GetLeaderShip() *LeaderShip {
	return leaderShip
}

func MustInitLeaderShip(client *clientv3.Client, leaderFunc, followerFunc func()) {
	opts := LeaderShipOption{
		client:       client,
		key:          "metaserver-leader",
		value:        config.GetConfig().Server.Addr,
		leaseTime:    10,
		renewalTime:  5,
		LeaderFunc:   leaderFunc,
		FollowerFunc: followerFunc,
	}

	l, err := NewLeaderShip(opts)
	if err != nil {
		log.Errorf("err: %v stack:%s", err, string(debug.Stack()))
		panic(fmt.Sprintf("init leadership failed, err: %v", err))
	}

	l.Start()

	leaderShip = l
}
