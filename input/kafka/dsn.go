package kafka

import (
	"regexp"
	"strings"
)

func ParseDSN(dsn string) map[string]string {
	var dsnPattern *regexp.Regexp
	dsnPattern = regexp.MustCompile(
		`(?:(?P<addr>[^\)]*)?)?` + // [addr]
			`\/(?P<topics>.*?)` + // /topics>.*?)`
			`(?:\?(?P<params>[^\?]*))?$`) // [?param1=value1&paramN=valueN]

	params := make(map[string]string)

	// 假如 dsn 只有 127.0.0.1:9092,192.168.1.100 不带 / 参数则认为只有 addr
	if !strings.Contains(dsn, "/") {
		params["addr"] = dsn
	}
	matches := dsnPattern.FindStringSubmatch(dsn)
	names := dsnPattern.SubexpNames()

	for i, match := range matches {
		switch names[i] {
		case "addr":
			params["addr"] = match
			break
		case "topics":
			params["topics"] = match
		case "params":
			for _, v := range strings.Split(match, "&") {
				param := strings.SplitN(v, "=", 2)
				if len(param) != 2 {
					continue
				}
				params[param[0]] = param[1]
			}
		}
	}
	return params
}
