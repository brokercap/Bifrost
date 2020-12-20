package xdb

import (
	"fmt"
	"log"
	"runtime/debug"
	"sync"
	"time"
)

type PoolClient struct {
	sync.RWMutex
	ClientChan         chan *Client
	Uri                string
	MaxClientCount     uint8
	CurrentClientCount uint8
	AvailableCount     uint8
}

var clientPool map[string]*PoolClient

func init() {
	clientPool = make(map[string]*PoolClient, 0)
}

func InitClientPool(name string, uri string, count uint8) error {
	if _, ok := clientPool[name]; !ok {
		clientPool[name] = &PoolClient{
			ClientChan:         make(chan *Client, int(count)),
			Uri:                uri,
			MaxClientCount:     count,
			CurrentClientCount: 0,
			AvailableCount:     0,
		}
	}
	return nil
}

func GetClient(name string) (c *Client, err error) {
	if _, ok := clientPool[name]; !ok {
		return nil, fmt.Errorf(name + " not esxit")
	}
	t := clientPool[name]
	t.Lock()
	if t.AvailableCount > 0 {
		t.AvailableCount--
		t.Unlock()
		//这里为什么不需要timeout,是因为前面加了lock 判断空闲连接数
		c = <-t.ClientChan
		return
	}
	if t.MaxClientCount > t.CurrentClientCount {
		t.CurrentClientCount++
		t.Unlock()
		f, stringKey := NewClient(name, t.Uri)
		if f == nil {
			t.Lock()
			t.CurrentClientCount--
			t.Unlock()
		}
		return f, stringKey
	}
	t.Unlock()
	timer := time.NewTimer(5 * time.Second)
	select {
	case c = <-t.ClientChan:
		break
	case <-timer.C:
		break
	}
	timer.Stop()
	if c == nil {
		return nil, fmt.Errorf("get client time out")
	}
	t.Lock()
	t.AvailableCount--
	t.Unlock()
	return
}

func BackCient(name string, c *Client) bool {
	defer func() {
		if err := recover(); err != nil {
			log.Println(string(debug.Stack()))
			return
		}
	}()
	if _, ok := clientPool[name]; !ok {
		return true
	}
	t := clientPool[name]
	t.Lock()
	if t.CurrentClientCount > t.MaxClientCount {
		t.CurrentClientCount--
		func() {
			defer func() {
				if err := recover(); err != nil {
					log.Println(string(debug.Stack()))
					return
				}
			}()
			//调用插件函数,关闭连接,这里防止插件代码写得有问题,抛异常,所以这里需要recover一次
			c.Close()
		}()
	} else {
		t.AvailableCount++
		t.ClientChan <- c
	}
	t.Unlock()
	return true
}
