package src

import (
	"encoding/json"
	"fmt"
	"github.com/brokercap/Bifrost/plugin/driver"
	"strings"
)

const VERSION = "v1.6.0"
const BIFROST_VERION = "v1.6.0"

func init() {
	driver.Register("TableCount", NewConn, VERSION, BIFROST_VERION)
}

type Conn struct {
	driver.PluginDriverInterface
	p              *PluginParam
	eventCountBool bool
}

type PluginParam struct {
	DbName string
}

func NewConn() driver.Driver {
	f := &Conn{}
	return f
}

func (This *Conn) SetOption(uri *string, param map[string]interface{}) {
	return
}

func (This *Conn) Open() error {
	return nil
}

func (This *Conn) GetUriExample() string {
	return "TableCount"
}

func (This *Conn) CheckUri() error {
	return nil
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

func (This *Conn) Close() bool {
	return true
}

func (This *Conn) Insert(data *driver.PluginDataType, retry bool) (*driver.PluginDataType, *driver.PluginDataType, error) {
	This.eventCountBool = true
	if data.BinlogPosition == 0 {
		This.eventCountBool = false
	}
	AddCount(This.p.DbName, data.SchemaName, data.TableName, INSERT, len(data.Rows), This.eventCountBool)
	return nil, nil, nil
}

func (This *Conn) Update(data *driver.PluginDataType, retry bool) (*driver.PluginDataType, *driver.PluginDataType, error) {
	This.eventCountBool = true
	if data.BinlogPosition == 0 {
		This.eventCountBool = false
	}
	AddCount(This.p.DbName, data.SchemaName, data.TableName, UPDATE, len(data.Rows)/2, This.eventCountBool)
	return nil, nil, nil
}

func (This *Conn) Del(data *driver.PluginDataType, retry bool) (*driver.PluginDataType, *driver.PluginDataType, error) {
	This.eventCountBool = true
	if data.BinlogPosition == 0 {
		This.eventCountBool = false
	}
	AddCount(This.p.DbName, data.SchemaName, data.TableName, DELETE, len(data.Rows), This.eventCountBool)
	return nil, nil, nil
}

func (This *Conn) Query(data *driver.PluginDataType, retry bool) (*driver.PluginDataType, *driver.PluginDataType, error) {
	if len(data.Query) >= 11 && strings.ToUpper(data.Query[0:11]) == "ALTER TABLE" {
		AddCount(This.p.DbName, data.SchemaName, data.TableName, DDL, 0, true)
	}
	return nil, nil, nil
}

func (This *Conn) Commit(data *driver.PluginDataType, retry bool) (LastSuccessCommitData *driver.PluginDataType, ErrData *driver.PluginDataType, err error) {
	AddCount(This.p.DbName, data.SchemaName, data.TableName, COMMIT, 0, true)
	return data, nil, nil
}
