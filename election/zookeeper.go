package election

import (
	"errors"
	"sort"
	"strings"
	"time"

	"fmt"

	"github.com/samuel/go-zookeeper/zk"
)

type SeesionExpireCb func()

type ChildrenEv struct {
	children []string
	ev       zk.Event
}

type ExistsEv struct {
	exists bool
	ev     zk.Event
}

var (
	ErrorNotFoundBackSvr = errors.New("back svr is illegal")
	ErrorIsMinimumNode   = errors.New("i am the minimum node")
	eventChanSize        = 8
)

type ZooKeeper struct {
	conn            *zk.Conn
	servers         []string
	sessionTimeout  time.Duration
	lock            *zk.Lock
	defaultEvent    <-chan zk.Event
	sessionexpired  bool
	onSessionExpire SeesionExpireCb
}

//NewZookeeper init ZooKeeper config
func NewZooKeeper(servers []string, sessionTimeout time.Duration) (*ZooKeeper, error) {
	// to do
	z := &ZooKeeper{nil, servers, sessionTimeout, nil, nil, false, nil}
	c, ev, err := zk.Connect(z.servers, sessionTimeout)
	if err != nil {
		return nil, err
	}
	z.conn = c
	z.defaultEvent = ev
	go z.defaultEventCallBack()
	return z, nil
}

func (z *ZooKeeper) SetSessionExpireCallBack(sessionExpire SeesionExpireCb) {
	z.onSessionExpire = sessionExpire
}
func (z *ZooKeeper) defaultEventCallBack() {
	for {
		select {
		case ec := <-z.defaultEvent:
			// 处理 session超时重连之后 重新注册temp节点和监视
			if ec.State == zk.StateHasSession && ec.Type == zk.EventSession && z.sessionexpired {
				//fmt.Println("need ++++++")
				if z.onSessionExpire != nil {
					z.onSessionExpire()
				}
				z.sessionexpired = false
			} else if ec.State == zk.StateExpired && ec.Type == zk.EventSession {
				z.sessionexpired = true
			}
		}
	}
}

func (z *ZooKeeper) ExistsWatchPreNode(path string, node string, rnode string) (<-chan zk.Event, error) {
	for {
		prenode, err := z.getPreNode(path, node, rnode)
		if err != nil {
			return nil, err
		}
		fmt.Println("get prenode ", prenode)
		exists, _, ch, err := z.conn.ExistsW(prenode)
		if exists {
			return ch, nil
		}
	}
}

func (z *ZooKeeper) isMinimunNode(path string, node string, rnode string) bool {
	children, _, err := z.conn.Children(path)
	if err != nil {
		return false
	}
	if len(children) <= 0 {
		// to do
		return false
	}
	index := strings.Index(rnode, node)
	if index >= 0 {
		rnode = rnode[index+len(node):]
	}
	for i, v := range children {
		index := strings.Index(v, node)
		if index < 0 {
			fmt.Println("err ", v, " rnode ", rnode, " node ", node)
			continue
		}
		children[i] = v[index+len(node):]
	}
	//fmt.Println("in isMinimunNode node ", node, " rnode ", rnode, " children ", children)
	sort.Strings(children)
	//fmt.Println("in isMinimunNode after sort  ", " children ", children)
	if rnode == children[0] {
		//fmt.Println("i am the smallest")
		return true
	}
	return false
}

func (z *ZooKeeper) getPreNode(path string, node string, rnode string) (string, error) {
	children, _, err := z.conn.Children(path)
	if err != nil {
		return "", err
	}
	if len(children) <= 0 {
		// to do
		return "", err
	}
	index := strings.Index(rnode, node)
	if index >= 0 {
		rnode = rnode[index+len(node):]
	}
	sc := make([]string, len(children))
	copy(sc, children)
	m := make(map[string]string, len(children))
	for i, v := range sc {
		index := strings.Index(v, node)
		if index < 0 {
			fmt.Println("err ", v, " rnode ", rnode, " node ", node)
			m[sc[i]] = children[i]
			continue
		}
		sc[i] = v[index+len(node):]
		m[sc[i]] = children[i]
	}
	fmt.Println("get m ", m)
	fmt.Println("in getPreNode node ", node, " rnode ", rnode, " children ", sc)
	sort.Strings(sc)
	fmt.Println("in getPreNode after sort  ", " children ", sc)
	for i, v := range sc {
		if rnode == v {
			if i > 0 {
				fmt.Println("get pre node  ", m[sc[i-1]])
				return path + "/" + m[sc[i-1]], nil
			} else {
				fmt.Println("get pre node  is myself ", i, v)
				err = ErrorIsMinimumNode
				return "", err
			}
		}
	}
	return "", err
}

//CreateZookeeper first create father path then create child path
func (z *ZooKeeper) CreateEphemeralNode(path, node, value string) (string, error) {
	pathSlice := strings.Split(path, "/")
	var deppath string
	for i := 0; i < len(pathSlice); i++ {
		if pathSlice[i] != "" {
			deppath += "/" + pathSlice[i]
			_, err := z.conn.Create(deppath, []byte(""), 0, zk.WorldACL(zk.PermAll))
			if err != nil && err != zk.ErrNodeExists {
				return "", err
			}
		}
	}
	fmt.Println(path + "/" + node)
	real_node, err := z.conn.CreateProtectedEphemeralSequential(path+"/"+node, []byte(value), zk.WorldACL(zk.PermAll))
	return real_node, err
}

//GetZookeeper path data
func (z *ZooKeeper) Get(path string) ([]byte, error) {
	data, _, err := z.conn.Get(path)
	return data, err
}

func (z *ZooKeeper) Close() {
	z.conn.Close()
}
