package plugin

import (
	"strconv"
	"log"
	"github.com/brokercap/Bifrost/plugin/driver"
	pluginStorage "github.com/brokercap/Bifrost/plugin/storage"
	"runtime/debug"
	"time"
	"sync"
)

type toServerChanContent struct {
	key  		string
	conn		driver.ConnFun
}

var l sync.RWMutex

var ToServerConnList map[string]map[string]driver.ConnFun
var toServerChanMap map[string]chan *toServerChanContent

func init()  {
	toServerChanMap = make(map[string]chan *toServerChanContent,0)
	ToServerConnList = make(map[string]map[string]driver.ConnFun)
}

func GetPlugin(ToServerKey string)  (driver.ConnFun, string){
	t := pluginStorage.GetToServerInfo(ToServerKey)
	if t == nil{
		log.Println("ToServer:",ToServerKey," no exsit,start error")
		return nil,""
	}
	var toServerChanContentData *toServerChanContent
	t.Lock()
	if t.AvailableConn > 0 {
		t.AvailableConn--
		t.Unlock()
		//这里为什么不需要timeout,是因为前面加了lock 判断空闲连接数
		toServerChanContentData = <-toServerChanMap[ToServerKey]
		return toServerChanContentData.conn,toServerChanContentData.key
	}
	if t.MaxConn > t.CurrentConn{
		t.CurrentConn++
		t.Unlock()
		f,stringKey := startPlugin(ToServerKey)
		if f == nil{
			t.Lock()
			t.CurrentConn--
			t.Unlock()
		}
		return f,stringKey
	}
	t.Unlock()
	timer := time.NewTimer(5 * time.Second)
	select {
	case toServerChanContentData = <-toServerChanMap[ToServerKey]:
		break
	case <- timer.C:
		break
	}
	timer.Stop()
	if toServerChanContentData == nil{
		return nil,""
	}
	t.Lock()
	t.AvailableConn--
	t.Unlock()
	return toServerChanContentData.conn,toServerChanContentData.key
}

func startPlugin(key string) (driver.ConnFun,string) {
	l.Lock()
	if _, ok := ToServerConnList[key]; !ok {
		ToServerConnList[key] = make(map[string]driver.ConnFun)
		toServerChanMap[key] = make(chan *toServerChanContent,500)
	}
	l.Unlock()

	t := pluginStorage.GetToServerInfo(key)
	if t == nil{
		return nil,""
	}
	var F driver.ConnFun
	var stringKey string
	F = driver.Open(t.PluginName,t.ConnUri)
	if F == nil{
		return nil,""
	}
	t.Lock()
	t.LastID++
	stringKey = strconv.Itoa(t.LastID)
	t.Unlock()

	l.Lock()
	ToServerConnList[key][stringKey] = F
	l.Unlock()
	return F,stringKey
}

func BackPlugin(ToServerKey string,key string,toServer driver.ConnFun) bool {
	defer func() {
		if err := recover();err !=nil{
			log.Println(string(debug.Stack()))
			return
		}
	}()
	t := pluginStorage.GetToServerInfo(ToServerKey)
	if t == nil{
		return true
	}
	t.Lock()
	if t.CurrentConn > t.MaxConn{
		t.CurrentConn--
		func(){
			defer func() {
				if err := recover();err != nil{
					log.Println(string(debug.Stack()))
					return
				}
			}()
			//调用插件函数,关闭连接,这里防止插件代码写得有问题,抛异常,所以这里需要recover一次
			ToServerConnList[ToServerKey][key].Close()
		}()
		delete(ToServerConnList[ToServerKey],key)
		l.Unlock()
	}else{
		t.AvailableConn++
		toServerChanMap[ToServerKey] <- &toServerChanContent{key:key,conn:toServer}
	}
	t.Unlock()
	return true
}