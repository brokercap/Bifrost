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

// 多少毫秒内有数据的情况下，写入 FileQueueUsableCount 次 内存队列后，队列都是满的状态 ，则启用 文件队列
var FileQueueUsableCountTimeDiff int64 = 5000

// 配置 FileQueueUsableCountTimeDiff 参数 使用
var FileQueueUsableCount uint32 = 10

// 在没有数据的情况下,间隔多久提交一次插件,单位 秒
var PluginCommitTimeOut int = 5

// 在同步出错的情况下,每2次重试之后 间隔多久再重试 ,单位 秒
var PluginSyncRetrycTime int = 5

// Bifrost 根目录文件夹绝对路径
var BifrostDir string = ""