package src

import (
	"encoding/json"
	"fmt"
	"github.com/bradfitz/gomemcache/memcache"
	"github.com/brokercap/Bifrost/plugin/driver"
)

const VERSION = "v1.6.0"
const BIFROST_VERION = "v1.6.0"

func init() {
	driver.Register("memcache", NewConn, VERSION, BIFROST_VERION)
}

type Conn struct {
	driver.PluginDriverInterface
	uri    *string
	status string
	conn   *memcache.Client
	err    error
	p      *PluginParam
}

type PluginParam struct {
	KeyConfig string
	Expir     int32
	DataType  string
	ValConfig string
}

func NewConn() driver.Driver {
	f := &Conn{
		status: "close",
	}
	return f
}

func (This *Conn) SetOption(uri *string, param map[string]interface{}) {
	This.uri = uri
	return
}

func (This *Conn) Open() error {
	This.Connect()
	return nil
}

func (This *Conn) GetUriExample() string {
	return "127.0.0.1:11211"
}

func (This *Conn) CheckUri() error {
	This.Connect()
	if This.err != nil {
		return This.err
	}
	This.Close()
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

func (This *Conn) Connect() bool {
	This.conn = memcache.New(*This.uri)
	if This.conn == nil {
		This.err = fmt.Errorf("memcache New failed %+v", *This.uri)
		return false
	}
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
	This.Connect()
	return true
}

func (This *Conn) Close() bool {
	return false
}

func (This *Conn) getKeyVal(data *driver.PluginDataType, index int) string {
	return fmt.Sprint(driver.TransfeResult(This.p.KeyConfig, data, index))
}

func (This *Conn) getVal(data *driver.PluginDataType, index int) string {
	return fmt.Sprint(driver.TransfeResult(This.p.ValConfig, data, index))
}

func (This *Conn) Insert(data *driver.PluginDataType, retry bool) (*driver.PluginDataType, *driver.PluginDataType, error) {
	return This.Update(data, retry)
}

func (This *Conn) Update(data *driver.PluginDataType, retry bool) (*driver.PluginDataType, *driver.PluginDataType, error) {
	if This.err != nil {
		This.ReConnect()
	}
	index := len(data.Rows) - 1
	Key := This.getKeyVal(data, index)
	var Val []byte
	if This.p.ValConfig != "" {
		Val = []byte(This.getVal(data, index))
	} else {
		p := data.Rows[index]
		Val, _ = json.Marshal(p)
	}
	var err error
	err = This.conn.Set(&memcache.Item{Key: Key, Value: Val, Expiration: This.p.Expir})

	if err != nil {
		This.err = err
		return nil, data, err
	}
	return nil, nil, nil
}

func (This *Conn) Del(data *driver.PluginDataType, retry bool) (*driver.PluginDataType, *driver.PluginDataType, error) {
	if This.err != nil {
		This.ReConnect()
	}
	Key := This.getKeyVal(data, 0)
	var err error
	err = This.conn.Delete(Key)
	if err != nil {
		This.err = err
		return nil, data, err
	}
	return nil, nil, nil
}

func (This *Conn) Query(data *driver.PluginDataType, retry bool) (*driver.PluginDataType, *driver.PluginDataType, error) {
	return nil, nil, nil
}

func (This *Conn) Commit(data *driver.PluginDataType, retry bool) (*driver.PluginDataType, *driver.PluginDataType, error) {
	return data, nil, nil
}
