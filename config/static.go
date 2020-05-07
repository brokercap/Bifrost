package config

var ToServerQueueSize int = 5000
var ChannelQueueSize int = 1000
var CountQueueSize int = 3000
var KeyCachePoolSize int = 50

var TLS bool = false

var TLSServerKeyFile string = ""
var TLSServerCrtFile string = ""

var DataDir = ""

// 是否开启文件队列，false 的话将不会启动文件队列功能
var FileQueueUsable bool = true
