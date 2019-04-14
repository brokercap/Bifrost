package plugin

import (
	"strconv"
	"log"
	"github.com/jc3wish/Bifrost/plugin/driver"
	"runtime/debug"
	"time"
)

type toServerChanContent struct {
	key  		string
	conn		driver.ConnFun
}

var ToServerConnList map[string]map[string]driver.ConnFun
var toServerChanMap map[string]chan *toServerChanContent

func init()  {
	toServerChanMap = make(map[string]chan *toServerChanContent,0)
	ToServerConnList = make(map[string]map[string]driver.ConnFun)
}

func GetPlugin(ToServerKey string)  (driver.ConnFun, string){
	log.Println("GetPlugin start")
	defer func() {
		log.Println("GetPlugin over")
	}()
	if _,ok := ToServerMap[ToServerKey];!ok{
		log.Println("ToServer:",ToServerKey," no exsit,Start error")
		return nil,""
	}
	t := ToServerMap[ToServerKey]
	var toServerChanContentData *toServerChanContent
	t.Lock()
	if t.AvailableConn > 0 {
		t.Unlock()
		//这里为什么不需要timeout,是因为前面加了lock 判断空闲连接数
		toServerChanContentData = <-toServerChanMap[ToServerKey]
		t.Lock()
		t.AvailableConn--
		t.Unlock()
		return toServerChanContentData.conn,toServerChanContentData.key
	}
	if t.MaxConn > t.CurrentConn{
		t.Unlock()
		f,stringKey := startPlugin(ToServerKey)
		return f,stringKey
	}
	t.Unlock()

	select {
	case toServerChanContentData = <-toServerChanMap[ToServerKey]:
		break
	case <- time.After(5 * time.Second):
		break
	}
	if toServerChanContentData == nil{
		return nil,""
	}
	t.Lock()
	t.AvailableConn--
	t.Unlock()
	return toServerChanContentData.conn,toServerChanContentData.key
}

func startPlugin(key string) (driver.ConnFun,string) {
	log.Println("startPlugin start")
	defer func() {
		log.Println("startPlugin over")
	}()
	l.Lock()
	if _, ok := ToServerConnList[key]; !ok {
		ToServerConnList[key] = make(map[string]driver.ConnFun)
		toServerChanMap[key] = make(chan *toServerChanContent,500)
	}
	l.Unlock()
	var F driver.ConnFun
	var stringKey string
	F = driver.Open(ToServerMap[key].PluginName,ToServerMap[key].ConnUri)
	if F == nil{
		return nil,""
	}
	t := ToServerMap[key]
	t.Lock()
	t.LastID++
	t.CurrentConn++
	stringKey = strconv.Itoa(t.LastID)
	t.Unlock()

	l.Lock()
	ToServerConnList[key][stringKey] = F
	l.Unlock()
	F.Connect()
	return F,stringKey
}

func BackPlugin(ToServerKey string,key string,toServer driver.ConnFun) bool {
	defer func() {
		if err := recover();err !=nil{
			log.Println(string(debug.Stack()))
			return
		}
	}()
	l.RLock()
	if _,ok := ToServerMap[ToServerKey];!ok{
		l.RUnlock()
		return true
	}
	l.RUnlock()
	t:=ToServerMap[ToServerKey]
	t.Lock()
	if t.CurrentConn > t.MaxConn{
		l.Lock()
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
		toServerChanMap[ToServerKey] <- &toServerChanContent{key:key,conn:toServer}
		t.AvailableConn++
	}
	t.Unlock()
	return true
}