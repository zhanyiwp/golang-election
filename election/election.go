package election

import (
	"context"
	"fmt"
	"sync"
	"time"
)

const (
	defaultLeaseTime     = 4 * time.Second
	defaultCheckInterval = 200 * time.Millisecond
	defaultWaitTime      = defaultLeaseTime + 2*defaultCheckInterval
)

type ChangeRoleCallBack func()

type LeaderChangeEv struct {
	Isleader bool
	Leader   string
}

type Elector struct {
	z                   *ZooKeeper
	path                string
	value               string
	node                string
	rnode               string
	isleader            bool
	leader              string
	m                   sync.RWMutex
	leasetime           int64
	lev                 chan LeaderChangeEv
	cancel              context.CancelFunc
	mc                  sync.Mutex
	onChangeToLeader    ChangeRoleCallBack
	onChangeToCandidate ChangeRoleCallBack
}

func NewElector(servers []string, sessionTimeout time.Duration, path, node, value string) (*Elector, error) {
	z, err := NewZooKeeper(servers, sessionTimeout)
	if err != nil {
		return nil, err
	}
	ev := make(chan LeaderChangeEv, eventChanSize)
	return &Elector{z: z,
		path:                path,
		node:                node,
		value:               value,
		isleader:            false,
		leasetime:           0,
		lev:                 ev,
		cancel:              nil,
		onChangeToLeader:    nil,
		onChangeToCandidate: nil}, nil
}

func (e *Elector) Start() (<-chan LeaderChangeEv, error) {
	e.z.SetSessionExpireCallBack(e.onSeesionExpire)
	var err error
	e.rnode, err = e.registerNode()
	if err != nil {
		fmt.Println("registerNode failed ", err)
		return nil, err
	}
	if e.isMinimunNode() {
		e.becomeLeader()
	} else {
		ctx, cancel := context.WithCancel(context.Background())
		e.mc.Lock()
		e.cancel = cancel
		e.mc.Unlock()
		go e.watchPreNode(ctx)
	}
	return e.lev, nil
}

func (e *Elector) registerNode() (string, error) {
	return e.z.CreateEphemeralNode(e.path, e.node, e.value)
}

func (e *Elector) isMinimunNode() bool {
	return e.z.isMinimunNode(e.path, e.node, e.rnode)
}

func (e *Elector) becomeLeader() {
	c := time.After(defaultWaitTime)
	// 确保之前的leader已经不再是leader
	fmt.Println("in becomeLeader need wait lease time ", defaultWaitTime)
	<-c
	fmt.Println("in becomeLeader nchange to leader")
	//再次确认竞选,防止自己在等待的过程中丢失leader资格造成的误选
	if e.isMinimunNode() {
		bchange := e.setLeader(true)
		e.setLeader(true)
		go e.leaseCheck()
		//leader = true
		if bchange {
			fmt.Println(" in becomeLeader bchange ")
			e.onChangeRole(true)
			select {
			case e.lev <- LeaderChangeEv{true, ""}:
			default:
				panic("zk: event channel full - it must be monitored and never allowed to be full")
			}
		}
		fmt.Println("in becomeLeader start become leader ")
	}

}

func (e *Elector) onChangeRole(leader bool) {
	if leader {
		if e.onChangeToLeader != nil {
			e.onChangeToLeader()
		}
	} else {
		if e.onChangeToCandidate != nil {
			e.onChangeToCandidate()
		}
	}
}

func (e *Elector) watchPreNode(ctx context.Context) {
	ev, err := e.z.ExistsWatchPreNode(e.path, e.node, e.rnode)
	if err != nil && err.Error() == "i am the minimum node" {
		//fmt.Println("i am the minimum node")
		e.becomeLeader()
		return
	}
	select {
	case <-ev:
		//fmt.Println("comer here+++++++++++++++")
		e.dealWatchEv(ctx)
	case <-ctx.Done():
		//fmt.Println("watchPreNode end")
	}
}

func (e *Elector) IsLeader() bool {
	e.m.RLock()
	isleader := e.isleader
	e.m.RUnlock()
	return isleader
}

func (e *Elector) dealWatchEv(ctx context.Context) {
	bchange, leader := false, false
	if e.z.isMinimunNode(e.path, e.node, e.rnode) {
		c := time.After(defaultWaitTime)
		// 确保之前的leader已经不再是leader
		fmt.Println("need wait lease time ", defaultWaitTime)
		<-c
		fmt.Println("nchange to leader")
		//再次确认竞选,防止自己在等待的过程中丢失leader资格造成的误选
		if e.z.isMinimunNode(e.path, e.node, e.rnode) {
			// beLeader
			bchange = e.setLeader(true)
			go e.leaseCheck()
			leader = true
			if bchange {
				fmt.Println("bchange ")
				e.onChangeRole(leader)
				fmt.Println("lalal ", leader)
				select {
				case e.lev <- LeaderChangeEv{leader, ""}:
				default:
					panic("zk: event channel full - it must be monitored and never allowed to be full")
				}
			}
		}
	} else {
		fmt.Println("in dealWatchEv call watchPreNode")
		//beCandidate
		bchange = e.setLeader(false)
		leader = false
		if bchange {
			fmt.Println("bchange ")
			e.onChangeRole(leader)
			//fmt.Println("lalal ", leader)
			select {
			case e.lev <- LeaderChangeEv{leader, ""}:
			default:
				//panic("zk: event channel full - it must be monitored and never allowed to be full")
			}
		}
		e.watchPreNode(ctx)
	}
	return
}

func (e *Elector) setLeader(leader bool) (changerole bool) {
	e.m.Lock()
	isleader := e.isleader
	e.isleader = leader
	e.leasetime = time.Now().UnixNano() + int64(defaultLeaseTime)
	e.m.Unlock()
	if isleader != leader {
		changerole = true
	} else {
		changerole = false
	}
	return changerole
}

func (e *Elector) leaseCheck() {
	c := time.Tick(defaultCheckInterval)
	i := 0
	for {
		select {
		case <-c:
			i++
			if int(defaultCheckInterval)*i > int(defaultLeaseTime)/2 {
				e.renewal()
				i = 0
			}
			if e.leasetime < time.Now().UnixNano() {
				//beCandidate
				e.setLeader(false)
				e.giveUp()
				goto forend
			}
		}
	}
forend:
	fmt.Println("com end")
}

func (e *Elector) renewal() {

	if e.isMinimunNode() {
		e.leasetime = time.Now().UnixNano() + int64(defaultLeaseTime)
	}
	fmt.Println("now ", time.Now().Unix(), " leasetime ", e.leasetime)
}

func (e *Elector) giveUp() {
	err := e.z.conn.Delete(e.rnode, 0)
	if err != nil {
		return
	}
	//需要有一个时机来注册临时节点
	fmt.Println("++++++++++++++++++++++++++++++++++++++++++++++==become slave")
	e.mc.Lock()
	if e.cancel != nil {
		e.cancel()
		e.cancel = nil
	}
	e.mc.Unlock()
	e.Start()
}

func (e *Elector) onSeesionExpire() {
	fmt.Println("on session expire")
	e.mc.Lock()
	if e.cancel != nil {
		e.cancel()
		e.cancel = nil
	}
	e.mc.Unlock()
	e.Start()
}
