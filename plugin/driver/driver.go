package driver

import (
	"encoding/json"
	"fmt"
	"log"
	"reflect"
	"regexp"
	"runtime/debug"
	"strconv"
	"strings"
	"sync"
	"time"
)

const API_VERSION = "v2.0"

const RegularxEpression = `\{\$([a-zA-Z0-9\-\_\[\]\'\"]+)\}`
const RegularxEpressionKey = `([a-zA-Z0-9\-\_]+)`

var reqTagAll *regexp.Regexp
var reqTagKey *regexp.Regexp

func init() {
	reqTagAll, _ = regexp.Compile(RegularxEpression)
	reqTagKey, _ = regexp.Compile(RegularxEpressionKey)
}

type PluginDataType struct {
	Timestamp       uint32
	EventSize       uint32
	EventType       string
	Rows            []map[string]interface{}
	Query           string
	SchemaName      string
	TableName       string
	AliasSchemaName string
	AliasTableName  string
	BinlogFileNum   int
	BinlogPosition  uint32
	Gtid            string
	Pri             []string
	EventID         uint64
	ColumnMapping   map[string]string
}

func GetApiVersion() string {
	return API_VERSION
}

type NewDriver func() Driver

type Driver interface {
	SetOption(uri *string, param map[string]interface{})
	Open() error
	Close() bool
	GetUriExample() string
	CheckUri() error
	Insert(data *PluginDataType, retry bool) (*PluginDataType, *PluginDataType, error) //binlog位点，处理了多少条数据,错误信息
	Update(data *PluginDataType, retry bool) (*PluginDataType, *PluginDataType, error)
	Del(data *PluginDataType, retry bool) (*PluginDataType, *PluginDataType, error)
	Query(data *PluginDataType, retry bool) (*PluginDataType, *PluginDataType, error)
	Commit(data *PluginDataType, retry bool) (*PluginDataType, *PluginDataType, error)
	SetParam(p interface{}) (interface{}, error)
	TimeOutCommit() (*PluginDataType, *PluginDataType, error)
	Skip(*PluginDataType) error
}

type DriverStructure struct {
	Version        string // 插件版本
	BifrostVersion string // 插件开发所使用的Bifrost的版本
	Error          string
	ExampleConnUri string
	driver         NewDriver
}

var (
	driversMu sync.RWMutex
	drivers   = make(map[string]DriverStructure)
)

func Register(name string, NewDriverFun NewDriver, version string, bifrost_version string) {
	defer func() {
		if err := recover(); err != nil {
			log.Println("plugin driver Register name:", name, " recory:", err, string(debug.Stack()))
		}
	}()
	driversMu.Lock()
	defer driversMu.Unlock()
	if NewDriverFun == nil {
		panic("Register driver is nil")
	}
	if _, ok := drivers[name]; ok {
		panic("Register called twice for driver " + name)
	}
	newDriver := NewDriverFun()
	drivers[name] = DriverStructure{
		Version:        version,
		BifrostVersion: bifrost_version,
		Error:          "",
		ExampleConnUri: newDriver.GetUriExample(),
		driver:         NewDriverFun,
	}
}

func Drivers() map[string]DriverStructure {
	driversMu.RLock()
	defer driversMu.RUnlock()
	//json 一次是为了重新拷贝一个内存空间的map出来,防止外部新增修改
	s, err := json.Marshal(drivers)
	if err != nil {
		return make(map[string]DriverStructure, 0)
	}
	var data map[string]DriverStructure
	json.Unmarshal(s, &data)
	return data
}

func Open(name string, uri *string) Driver {
	driversMu.RLock()
	defer driversMu.RUnlock()
	if _, ok := drivers[name]; !ok {
		return nil
	}
	newDriver := drivers[name].driver()
	newDriver.SetOption(uri, nil)
	newDriver.Open()
	return newDriver
}

func CheckUri(name string, uri *string) error {
	driversMu.RLock()
	defer driversMu.RUnlock()
	if _, ok := drivers[name]; !ok {
		return fmt.Errorf("no " + name)
	}
	newDriver := drivers[name].driver()
	newDriver.SetOption(uri, nil)
	err := newDriver.CheckUri()
	return err
}

func TransfeResult(val string, data *PluginDataType, rowIndex int, noTagsReturnNil ...bool) interface{} {
	if data == nil {
		return nil
	}
	p := reqTagAll.FindAllStringSubmatch(val, -1)
	// 假如不存在 {$json} 标签，则直接返回nil
	// 这个改动,在极端情况下,可能会导致以下版本升级至此合并之后的版本在下面场景使用情况下,出现不一致的结果
	// 2.0.6-2.2.1,2.3.0,2.3.1
	// 特殊场景:
	// 目标端为mysql,clickhouse等目标端,并且界面上强制绑定字段映射关系的情况下
	// 比如目标端绑定了一个在源端不存在的字段 xxx, 在以上列出来的版本中,则直接返回 xxx
	// 但升级到此提交后的版本,则直接返回nil,
	// 如果要达到不存在,则直接xxx,而不返回nil的情况,则需要在目标端插件代码中修改
	// 或者 将字段绑定关系中配置成 xxx{$BifrostNull} ,增加一个{$BifrostNull}标签,也可达到一样的效果,{$BifrostNull} 会被替换成空字符串
	// 此提交版本后使用案例
	// 案例: data 中有字段及值 x1:"v1",x2:"v2",x3:"v3"
	// 情况1:  val = "x4" , 返回 nil or x4 ,取决于 noTagsReturnNil 是否为 true,
	// 这里是为了兼容多个插件,比如kafka插件topic,用户填写的是没使用{$} tag的值,但是Mysql,ck等插件字段绑定又使用不存在的字段绑定
	// 情况2:  val = "x4{BifrostNull}" ,返回 x4
	// 情况3:  val = "x1" , 返回 x1对应的的值v1
	// 情况4:  val = {BifrostNull} , 返回 nil
	if len(p) == 0 {
		if len(noTagsReturnNil) > 0 && noTagsReturnNil[0] {
			return nil
		}
		return val
	}
	var n int
	if data.Rows == nil {
		n = -1
	} else {
		n = len(data.Rows) - 1
	}
	for _, v := range p {
		switch v[1] {
		case "TableName":
			val = strings.Replace(val, "{$TableName}", data.TableName, -1)
			break
		case "SchemaName":
			val = strings.Replace(val, "{$SchemaName}", data.SchemaName, -1)
			break
		case "EventType":
			val = strings.Replace(val, "{$EventType}", data.EventType, -1)
			break
		case "Timestamp":
			val = strings.Replace(val, "{$Timestamp}", fmt.Sprint(time.Now().Unix()), -1)
			break
		case "BinlogTimestamp":
			val = strings.Replace(val, "{$BinlogTimestamp}", fmt.Sprint(data.Timestamp), -1)
			break
		case "BinlogDateTime":
			t := time.Unix(int64(data.Timestamp), 0)
			val = t.Format("2006-01-02 15:04:05")
			break
		case "BinlogFileNum":
			val = strings.Replace(val, "{$BinlogFileNum}", fmt.Sprint(data.BinlogFileNum), -1)
			break
		case "BinlogPosition":
			val = strings.Replace(val, "{$BinlogPosition}", fmt.Sprint(data.BinlogPosition), -1)
			break
		case "GTID":
			val = strings.Replace(val, "{$GTID}", fmt.Sprint(data.Gtid), -1)
			break
		case "BifrostNull":
			if val == "{$BifrostNull}" {
				return nil
			}
			val = strings.Replace(val, "{$BifrostNull}", "", -1)
			break
		default:
			if rowIndex <= n && rowIndex >= 0 {
				// 如数据中 标签是整个字段，则自直接返回字段内容
				if _, ok := data.Rows[rowIndex][v[1]]; ok {
					val = strings.Replace(val, v[0], fmt.Sprint(data.Rows[rowIndex][v[1]]), -1)
					break
				}
				// 将标签 {$json['key1'][0]['key2']} 转成  json  key1 0 key2, json 必须是表字段名
				// 假如 json 并不是 表里的字段 或者 只有 {$json} 这样一个标签的情况下(因为上面已经匹配过这个字段是不是表字段了)，则不对这个标签进行做任务替换处理
				p2 := reqTagKey.FindAllStringSubmatch(v[1], -1)
				if len(p2) == 1 {
					break
				}
				if _, ok := data.Rows[rowIndex][p2[0][1]]; !ok {
					if val == v[0] {
						break
					}
				}
				var d reflect.Value
				d = reflect.ValueOf(data.Rows[rowIndex])
				for _, v2 := range p2 {
					if !d.IsValid() {
						continue
					}
					if d.Kind() == reflect.Interface {
						d = reflect.ValueOf(d.Interface())
					}
					switch d.Kind() {
					case reflect.Array, reflect.Slice:
						key, err := strconv.Atoi(v2[1])
						if err != nil {
							d = reflect.ValueOf(nil)
							continue
						}
						if d.Len()-1 < key {
							d = reflect.ValueOf(nil)
							continue
						}
						d = d.Index(key)
						break
					case reflect.Map:
						d = d.MapIndex(reflect.ValueOf(v2[1]))
						break
					default:
						d = reflect.ValueOf(nil)
						break
					}
				}
				if !d.IsValid() {
					if val == v[0] {
						return nil
					} else {
						val = strings.Replace(val, v[0], fmt.Sprint(nil), -1)
					}
				} else {
					if val == v[0] {
						return d.Interface()
					} else {
						val = strings.Replace(val, v[0], fmt.Sprint(d), -1)
					}
				}
			} else {
				if val == v[0] {
					return nil
				} else {
					val = strings.Replace(val, v[0], fmt.Sprint(nil), -1)
				}
			}
			break
		}
	}
	return val
}
