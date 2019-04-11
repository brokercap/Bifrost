package warning

import (
	"time"
	"sync"
	"encoding/json"
	"net"
	"log"
	"runtime/debug"
	"fmt"
)

var l sync.RWMutex

var IP string = ""

type WarningType string

const (
	WARNINGERROR WarningType = "ERROR"
	WARNINGNORMAL WarningType = "NORMAL"
)

type WarningContent struct {
	Type 		WarningType
	DbName 		string
	SchemaName  string
	TableName   string
	Channel		string
	Body 		interface{}
	DateTime 	string
	IP   		string
}

var WarningChan chan WarningContent

func init()  {
	WarningChan = make(chan WarningContent,500)
	IP = getIP()
	go consumeWarning()
}

//新增报警内容
func AppendWarning(data WarningContent){
	//这里为什么 要有一个写入超时，是为了防止 chan 满了情况下阻塞，然后影响了正常数据同步
	select {
		case WarningChan <- data:
		break
	case <-time.After(2 * time.Minute):
		break
	}
}

func getIP() string{
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		log.Println("getIP err:",err)
		return ""
	}

	for _, address := range addrs {
		// 检查ip地址判断是否回环地址
		if ipnet, ok := address.(*net.IPNet); ok && !ipnet.IP.IsLoopback() {
			if ipnet.IP.To4() != nil {
				return ipnet.IP.To4().String()
			}
		}
	}
	return ""
}

func getWarningBody(data WarningContent) string{
	if data.DateTime == ""{
		data.DateTime = time.Now().Format("2006-01-02 15:04:05")
	}
	data.IP = IP
	b,_ := json.Marshal(data)
	return string(b)
}

func consumeWarning(){
	for{
		select {
		case data := <- WarningChan:
			body := getWarningBody(data)
			l.RLock()
			for _,config := range allWaringConfigCacheMap{
				sendToWaring(config,body,5)
			}
			l.RUnlock()
			break
		case <-time.After(30 * time.Minute):
			break
		}
	}
}


func sendToWaring(config WaringConfig,c string,n int){
	defer func() {
		if err:=recover();err!=nil{
			log.Println(string(debug.Stack()))
		}
	}()
	if _,ok := dirverMap[config.Type];!ok{
		return
	}

	for i:=0; i< n; i++{
		err := dirverMap[config.Type].SendWarning(config.Param,c)
		if err == nil{
			return
		}
		time.Sleep(5 * time.Second)
	}
}


func CheckWarngConfigBySendTest(config WaringConfig,c string) error{
	defer func() {
		if err:=recover();err!=nil{
			log.Println(string(debug.Stack()))
		}
	}()
	if _,ok := dirverMap[config.Type];!ok{
		return fmt.Errorf("Type:"+config.Type + "not exsit")
	}

	err := dirverMap[config.Type].SendWarning(config.Param,c)
	return err
}