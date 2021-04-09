package plugin

import (
	"fmt"
	"github.com/brokercap/Bifrost/plugin/driver"
	pluginStorage "github.com/brokercap/Bifrost/plugin/storage"
	"log"
	"runtime/debug"
	"sync"
	"time"
)

type ToServerConn struct {
	id          int
	toServerKey string
	conn        driver.Driver
	updateTime  int64
}

var l sync.RWMutex

var ToServerConnList map[string]map[int]*ToServerConn
var toServerChanMap map[string]chan *ToServerConn

func init() {
	toServerChanMap = make(map[string]chan *ToServerConn, 0)
	ToServerConnList = make(map[string]map[int]*ToServerConn)
}

func (This *ToServerConn) GetConn() driver.Driver {
	return This.conn
}

func (This *ToServerConn) checkClose(t *pluginStorage.ToServer) {
	if t.UpdateTime != This.updateTime {
		defer func() {
			if err := recover(); err != nil {
				log.Println("ToServerKey:", This.toServerKey, "close recover:", err, string(debug.Stack()))
			}
			This.updateTime = t.UpdateTime
		}()
		This.conn.Close()
	}
}

func GetPlugin(ToServerKey string) (toServerConn *ToServerConn) {
	t := pluginStorage.GetToServerInfo(ToServerKey)
	if t == nil {
		log.Println("ToServer:", ToServerKey, " no exsit,start error")
		return nil
	}
	t.Lock()
	// 有空闲连接,并且空闲连接大于 配置的最小连接数的时候，直接从空闲连接中拿取
	if t.AvailableConn >= t.MinConn {
		t.AvailableConn--
		t.Unlock()
		//这里为什么不需要timeout,是因为前面加了lock 判断空闲连接数
		toServerConn = <-toServerChanMap[ToServerKey]
		toServerConn.checkClose(t)
		return toServerConn
	}
	// 在没有空闲连接的情况，并且 连接数还没达最大值，则直接创建连新
	if t.MaxConn > t.CurrentConn {
		// 这里要提前先 释放 t.Unloc 的锁，减少锁等待等问题
		// 这里先 给 CurrentConn +1
		// 待后面 startPlugin 失败的情况下，再 -1，保留数据是准确的
		t.CurrentConn++
		t.Unlock()
		toServerConn = startPlugin(ToServerKey)
		if toServerConn == nil {
			t.Lock()
			t.CurrentConn--
			t.Unlock()
		}
		return toServerConn
	}
	t.Unlock()
	// 执行到了，说明 没有空闲连接 并且 连接数也达最大值了，那只有阻塞等待 其他地方释放连接进来了
	// 阻塞等待连接，也只等待 5 秒，超过这个数，则直接返回 nil, 让上一层去决定是否要重新获取
	timer := time.NewTimer(5 * time.Second)
	select {
	case toServerConn = <-toServerChanMap[ToServerKey]:
		break
	case <-timer.C:
		break
	}
	timer.Stop()
	if toServerConn == nil {
		return nil
	}
	t.Lock()
	t.AvailableConn--
	t.Unlock()
	toServerConn.checkClose(t)
	return toServerConn
}

func startPlugin(ToServerKey string) (toServerConn *ToServerConn) {
	l.Lock()
	if _, ok := toServerChanMap[ToServerKey]; !ok {
		ToServerConnList[ToServerKey] = make(map[int]*ToServerConn)
		toServerChanMap[ToServerKey] = make(chan *ToServerConn, 512)
	}
	l.Unlock()

	t := pluginStorage.GetToServerInfo(ToServerKey)
	if t == nil {
		return nil
	}
	var F driver.Driver
	var ConnId int
	F = driver.Open(t.PluginName, &t.ConnUri)
	if F == nil {
		return nil
	}
	t.Lock()
	t.LastID++
	ConnId = t.LastID
	t.Unlock()
	toServerConn = &ToServerConn{
		id:          ConnId,
		toServerKey: ToServerKey,
		conn:        F,
		updateTime:  t.UpdateTime,
	}
	l.Lock()
	ToServerConnList[ToServerKey][ConnId] = toServerConn
	l.Unlock()
	return
}

func BackPlugin(ToServerConn *ToServerConn) bool {
	defer func() {
		if err := recover(); err != nil {
			log.Printf("BackPlugin ToServerKey:%s recover err:%s debug:%s", ToServerConn.toServerKey, fmt.Sprint(err), string(debug.Stack()))
			return
		}
	}()
	t := pluginStorage.GetToServerInfo(ToServerConn.toServerKey)
	if t == nil {
		return true
	}
	t.Lock()
	if t.CurrentConn > t.MaxConn {
		t.CurrentConn--
		func() {
			defer func() {
				if err := recover(); err != nil {
					log.Println(string(debug.Stack()))
					return
				}
			}()
			//调用插件函数,关闭连接,这里防止插件代码写得有问题,抛异常,所以这里需要recover一次
			ToServerConn.conn.Close()
		}()

		l.Lock()
		delete(ToServerConnList[ToServerConn.toServerKey], ToServerConn.id)
		l.Unlock()

	} else {
		t.AvailableConn++
		l.RLock()
		toServerChanMap[ToServerConn.toServerKey] <- ToServerConn
		l.RUnlock()
	}
	t.Unlock()
	return true
}
