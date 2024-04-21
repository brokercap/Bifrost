package warning

import (
	"encoding/json"
	"fmt"
	"log"
	"net"
	"runtime/debug"
	"sync"
	"time"
)

var l sync.RWMutex

var IP string = ""

type WarningType string

const (
	WARNINGERROR  WarningType = "ERROR"
	WARNINGNORMAL WarningType = "NORMAL"
)

type WarningContent struct {
	Type       WarningType
	DbName     string
	SchemaName string
	TableName  string
	Channel    string
	Body       interface{}
	DateTime   string
	IP         string
}

var WarningChan chan WarningContent

func init() {
	WarningChan = make(chan WarningContent, 500)
	IP = getIP()
	go consumeWarning()
}

// 新增报警内容
func AppendWarning(data WarningContent) {
	//这里为什么 要有一个写入超时，是为了防止 chan 满了情况下阻塞，然后影响了正常数据同步
	timer := time.NewTimer(2 * time.Second)
	select {
	case WarningChan <- data:
		break
	case <-timer.C:
		break
	}
	timer.Stop()
}

func getIP() string {
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		log.Println("getIP err:", err)
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

func getWarningBody(data WarningContent) string {
	if data.DateTime == "" {
		data.DateTime = time.Now().Format("2006-01-02 15:04:05")
	}
	data.IP = IP
	b, _ := json.Marshal(data)
	return string(b)
}

func consumeWarning() {
	timer := time.NewTimer(30 * time.Minute)
	defer timer.Stop()
	for {
		select {
		case data := <-WarningChan:
			InitWarningConfigCache()
			timer.Reset(30 * time.Minute)
			body := getWarningBody(data)
			var title string
			switch data.Type {
			case WARNINGERROR:
				title = "Bifrost Warning"
				break
			case WARNINGNORMAL:
				title = "Bifrost Return Normal"
				break
			default:
				title = "Bifrost Other Warning"
				break
			}
			l.RLock()
			for _, config := range allWaringConfigCacheMap {
				sendToWaring(config, title, body, 5)
			}
			l.RUnlock()
			break
		case <-timer.C:
			timer.Reset(30 * time.Minute)
			break
		}
	}
}

func sendToWaring(config WaringConfig, title, c string, n int) {
	defer func() {
		if err := recover(); err != nil {
			log.Println(string(debug.Stack()))
		}
	}()
	if _, ok := dirverMap[config.Type]; !ok {
		return
	}
	var err error
	for i := 0; i < n; i++ {
		err = dirverMap[config.Type].SendWarning(config.Param, title, c)
		if err == nil {
			return
		}
		time.Sleep(5 * time.Second)
	}
	if err != nil {
		log.Println("sendToWaring err:", err, "title:", title, "body:", c)
	}
}

func CheckWarngConfigBySendTest(config WaringConfig, c string) error {
	defer func() {
		if err := recover(); err != nil {
			log.Println(string(debug.Stack()))
		}
	}()
	if _, ok := dirverMap[config.Type]; !ok {
		return fmt.Errorf("Type:" + config.Type + "not exsit")
	}
	title := "Bifrost warning test"
	err := dirverMap[config.Type].SendWarning(config.Param, title, c)
	return err
}
