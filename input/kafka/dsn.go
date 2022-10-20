package kafka

import (
	"strings"
)

func ParseDSN(dsn string) (params map[string]string) {
	params = make(map[string]string, 0)
	if dsn == "" {
		return
	}
	var index int
	var addrAndTopics string
	var paramStr string
	index = strings.Index(dsn, "?")
	// 127.0.0.1:9092,192.168.1.100/topic1,topic2?param=p1&param2=p2
	// ==> 127.0.0.1:9092,192.168.1.100    topic1,topic2   param=p1   param2=p2
	//  127.0.0.1:9092,192.168.1.100?param=p1&param2=p2
	// ==> 127.0.0.1:9092,192.168.1.100  param=p1   param2=p2
	if index <= 0 {
		addrAndTopics = dsn
	} else {
		addrAndTopics = dsn[0:index]
		paramStr = dsn[index+1:]
	}
	addrAndTopicsArr := strings.Split(addrAndTopics, "/")
	params["addr"] = addrAndTopicsArr[0]
	if len(addrAndTopicsArr) > 1 {
		params["topics"] = addrAndTopicsArr[1]
	}
	if paramStr == "" {
		return
	}
	for _, v := range strings.Split(paramStr, "&") {
		param := strings.SplitN(v, "=", 2)
		if len(param) != 2 {
			continue
		}
		params[param[0]] = param[1]
	}
	return params
}
