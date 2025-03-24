package user

import (
	"github.com/brokercap/Bifrost/config"
	"sync"
	"time"
)

var failedIpCountMap map[string]int
var refuseIpMap map[string]int64
var refuseIpLock sync.RWMutex

func init() {
	failedIpCountMap = make(map[string]int, 0)
	refuseIpMap = make(map[string]int64, 0)
	if config.RefuseIpTimeOut > 0 {
		go crondDelRefuseIpByTimeOut()
	}
}

func crondDelRefuseIpByTimeOut() {
	timer := time.NewTimer(1 * time.Hour)
	defer timer.Stop()
	for {
		<-timer.C
		DelRefuseIpByTimeOut()
		timer.Reset(1 * time.Hour)
	}
}

func DelRefuseIpByTimeOut() {
	refuseIpLock.Lock()
	defer refuseIpLock.Unlock()
	var nowTime = time.Now().Unix()
	for k, v := range refuseIpMap {
		if nowTime-v > config.RefuseIpTimeOut {
			delete(refuseIpMap, k)
		}
	}
	// 这里强制也将 failedIpCountMap 统计清空一次，防止过多Ip登入,并且不再登入的情况下,内存空间没释放,这里有可能会造成部分数据丢失,设计是并不考滤这个极端情况
	failedIpCountMap = make(map[string]int, 0)
}

func AddFailedIp(ip string) {
	var NeedAddRefuseBool = false
	refuseIpLock.Lock()
	if n, ok := failedIpCountMap[ip]; ok {
		failedIpCountMap[ip]++
		if n >= config.RefuseIpLoginFailedCount {
			NeedAddRefuseBool = true
			delete(failedIpCountMap, ip)
		}
	} else {
		failedIpCountMap[ip] = 1
	}
	refuseIpLock.Unlock()
	if NeedAddRefuseBool {
		AddRefuseIp(ip)
		appendLoginLog("IP:%s AddToRefuseIp", ip)
	}
}

func DelFailedIp(ip string) {
	refuseIpLock.Lock()
	delete(failedIpCountMap, ip)
	refuseIpLock.Unlock()
}

func AddRefuseIp(ip string) {
	refuseIpLock.Lock()
	refuseIpMap[ip] = time.Now().Unix()
	refuseIpLock.Unlock()
}

func DelRefuseIp(ip string) {
	refuseIpLock.Lock()
	delete(refuseIpMap, ip)
	refuseIpLock.Unlock()
	appendLoginLog("IP:%s DelFailedIp", ip)
}

func CheckRefuseIp(ip string) bool {
	refuseIpLock.RLock()
	defer refuseIpLock.RUnlock()
	if lastRefuseTime, ok := refuseIpMap[ip]; ok {
		// 判断一次,是否已经超过自动释放的时间
		if time.Now().Unix()-lastRefuseTime > config.RefuseIpTimeOut {
			return false
		}
		return true
	}
	return false
}

func GetRefuseIpMap() map[string]string {
	data := make(map[string]string, 0)
	refuseIpLock.RLock()
	defer refuseIpLock.RUnlock()
	for k, v := range refuseIpMap {
		data[k] = time.Unix(v, 0).Format("2006-01-02 15:04:05")
	}
	return data
}
