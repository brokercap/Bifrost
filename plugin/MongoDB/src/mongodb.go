package src

import (
	"encoding/json"
	"fmt"
	pluginDriver "github.com/brokercap/Bifrost/plugin/driver"
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
	"log"
	"runtime/debug"
	"strings"
)

const VERSION = "v1.6.0"
const BIFROST_VERION = "v1.6.0"

func init() {
	pluginDriver.Register("MongoDB", NewConn, VERSION, BIFROST_VERION)
}

type Conn struct {
	pluginDriver.PluginDriverInterface
	Uri    *string
	status string
	conn   *mgo.Session
	err    error
	p      *PluginParam
}

type PluginParam struct {
	SchemaName  string
	TableName   string
	PrimaryKey  string
	primaryKeys []string
	hadIndexMap map[string]bool
	indexName   string
}

func NewConn() pluginDriver.Driver {
	f := &Conn{status: "close", err: fmt.Errorf("close")}
	return f
}

func (This *Conn) SetOption(uri *string, param map[string]interface{}) {
	This.Uri = uri
	return
}

func (This *Conn) Open() error {
	This.Connect()
	return nil
}

func (This *Conn) GetUriExample() string {
	return "[mongodb://][user:pass@]host1[:port1][,host2[:port2],...][/database][?options]"
}

func (This *Conn) CheckUri() error {
	This.Connect()
	if This.status == "running" {
		This.Close()
		return nil
	} else {
		return This.err
	}
}

func (This *Conn) GetParam(p interface{}) (*PluginParam, error) {
	s, err := json.Marshal(p)
	if err != nil {
		return nil, err
	}
	var param PluginParam
	err2 := json.Unmarshal(s, &param)
	if err2 != nil {
		return nil, err2
	}
	if param.SchemaName == "" || param.TableName == "" {
		return nil, fmt.Errorf("SchemaName,TableName can't be empty")
	}
	param.indexName = "bifrost_unique_index"
	param.primaryKeys = strings.Split(param.PrimaryKey, ",")
	param.hadIndexMap = make(map[string]bool, 0)
	This.p = &param
	return &param, nil
}

func (This *Conn) SetParam(p interface{}) (interface{}, error) {
	if p == nil {
		return nil, fmt.Errorf("param is nil")
	}
	switch p.(type) {
	case *PluginParam:
		This.p = p.(*PluginParam)
		return p, nil
	default:
		return This.GetParam(p)
	}
}

func (This *Conn) Connect() bool {
	var err error
	This.conn, err = mgo.Dial(*This.Uri)
	if err != nil {
		This.err = err
		This.status = "close"
		return false
	}
	This.conn.SetMode(mgo.Monotonic, true)
	This.err = nil
	This.status = "running"
	return true
}

func (This *Conn) ReConnect() bool {
	defer func() {
		if err := recover(); err != nil {
			This.err = fmt.Errorf(fmt.Sprint(err))
		}
	}()
	This.Close()
	This.Connect()
	return true
}

func (This *Conn) Close() bool {
	func() {
		defer func() {
			if err := recover(); err != nil {
				return
			}
		}()
		if This.conn != nil {
			This.conn.Close()
		}
	}()
	This.status = "close"
	This.conn = nil
	This.err = fmt.Errorf("close")
	return true
}

// 假如没有配置指定 PrimaryKey (mongodb 中的文档ID) 的时候，将 原表中的 Pri 主键当作 MongoDB 的文档ID
func (This *Conn) initPrimaryKeys(data *pluginDriver.PluginDataType) {
	if This.p.PrimaryKey == "" {
		This.p.primaryKeys = data.Pri
	}
}

func (This *Conn) createIndex(c *mgo.Collection) {
	indexTableKey := c.Database.Name + "#" + c.Name
	if _, ok := This.p.hadIndexMap[indexTableKey]; !ok {
		indexs, err := c.Indexes()
		if err == nil {
			//假如表里已经拥有了指定索引名称的索引，而不再创建索引
			//假如这里创建了2个字段的索引，用户又在mongodb server修改了这个索引，是很有可能会出问题的，使用的时候，需要注意
			for _, indexInfo := range indexs {
				if indexInfo.Name == This.p.indexName {
					This.p.hadIndexMap[indexTableKey] = true
					return
				}
			}
		}
		index := mgo.Index{Key: This.p.primaryKeys, Unique: true, Name: This.p.indexName}
		This.p.hadIndexMap[indexTableKey] = true
		c.EnsureIndex(index)
	}
}

func (This *Conn) Insert(data *pluginDriver.PluginDataType, retry bool) (LastSuccessCommitData *pluginDriver.PluginDataType, ErrData *pluginDriver.PluginDataType, e error) {
	if This.err != nil {
		This.Connect()
	}
	if This.err != nil {
		return nil, data, This.err
	}
	This.initPrimaryKeys(data)
	if len(This.p.primaryKeys) == 0 {
		return nil, data, fmt.Errorf("PrimaryKey is empty And Table No Pri!")
	}
	n := len(data.Rows) - 1
	SchemaName := fmt.Sprint(pluginDriver.TransfeResult(This.p.SchemaName, data, n))
	TableName := fmt.Sprint(pluginDriver.TransfeResult(This.p.TableName, data, n))
	/*
		if _,ok := data.Rows[n][This.p.PrimaryKey];!ok{
			return nil,data,fmt.Errorf("PrimaryKey "+ This.p.PrimaryKey +" is not exsit")
		}
	*/
	defer func() {
		if err := recover(); err != nil {
			LastSuccessCommitData = nil
			e = fmt.Errorf(string(debug.Stack()))
			This.err = e
			log.Println(e)
			return
		}
	}()
	c := This.conn.DB(SchemaName).C(TableName)
	This.createIndex(c)
	k := make(bson.M, 1)
	for _, key := range This.p.primaryKeys {
		if _, ok := data.Rows[n][key]; ok {
			k[key] = data.Rows[n][key]
		} else {
			return nil, data, fmt.Errorf("key:" + key + " no exsit")
		}
	}
	_, err := c.Upsert(k, data.Rows[n])
	if err != nil {
		return nil, data, err
	}
	return nil, nil, nil
}

func (This *Conn) Update(data *pluginDriver.PluginDataType, retry bool) (LastSuccessCommitData *pluginDriver.PluginDataType, ErrData *pluginDriver.PluginDataType, e error) {
	return This.Insert(data, retry)
}

func (This *Conn) Del(data *pluginDriver.PluginDataType, retry bool) (LastSuccessCommitData *pluginDriver.PluginDataType, ErrData *pluginDriver.PluginDataType, e error) {
	if This.err != nil {
		This.Connect()
	}
	if This.err != nil {
		return nil, data, This.err
	}
	This.initPrimaryKeys(data)
	if len(This.p.primaryKeys) == 0 {
		return nil, data, fmt.Errorf("PrimaryKey is empty And Table No Pri!")
	}
	defer func() {
		if err := recover(); err != nil {
			LastSuccessCommitData = nil
			e = fmt.Errorf(string(debug.Stack()))
			This.err = e
			log.Println(string(debug.Stack()))
			return
		}
	}()
	SchemaName := fmt.Sprint(pluginDriver.TransfeResult(This.p.SchemaName, data, 0))
	TableName := fmt.Sprint(pluginDriver.TransfeResult(This.p.TableName, data, 0))
	c := This.conn.DB(SchemaName).C(TableName)
	This.createIndex(c)
	k := make(bson.M, 1)
	for _, key := range This.p.primaryKeys {
		if _, ok := data.Rows[0][key]; ok {
			k[key] = data.Rows[0][key]
		} else {
			return nil, data, fmt.Errorf("key:" + key + " no exsit")
		}
	}
	err := c.Remove(k)
	if err != nil {
		return nil, data, err
	}
	return nil, nil, nil
}

func (This *Conn) Query(data *pluginDriver.PluginDataType, retry bool) (LastSuccessCommitData *pluginDriver.PluginDataType, ErrData *pluginDriver.PluginDataType, e error) {
	return data, nil, nil
}

func (This *Conn) Commit(data *pluginDriver.PluginDataType, retry bool) (LastSuccessCommitData *pluginDriver.PluginDataType, ErrData *pluginDriver.PluginDataType, e error) {
	return data, nil, nil
}
