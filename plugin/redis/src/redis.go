package src

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/brokercap/Bifrost/plugin/driver"
	//"github.com/go-redis/redis"
	"github.com/go-redis/redis/v8"
	"strconv"
	"strings"
	"time"
)

const VERSION = "v1.7.4"
const BIFROST_VERION = "v1.7.4"

func init() {
	driver.Register("redis", NewConn, VERSION, BIFROST_VERION)
}

var ctx = context.Background()

type Conn struct {
	driver.PluginDriverInterface
	Uri    *string
	status string
	conn   redis.UniversalClient
	err    error
	p      *PluginParam
}

type PluginParam struct {
	KeyConfig          string
	Expir              int
	DataType           string
	ValConfig          string
	Type               string
	BifrostFilterQuery bool // bifrost server 保留,是否过滤sql事件
}

func NewConn() driver.Driver {
	f := &Conn{
		status: "close",
	}
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
	return "pwd@tcp(127.0.0.1:6379)/0 or 127.0.0.1:6379 or pwd@tcp(127.0.0.1:6379,127.0.0.1:6380)/0 or 127.0.0.1:6379,127.0.0.1:6380"
}

func (This *Conn) CheckUri() error {
	This.Connect()
	if This.err != nil {
		return This.err
	}
	This.Close()
	return nil
}

func GetUriParam(uri string) (pwd string, network string, url string, database int) {
	i := strings.LastIndex(uri, "@")
	pwd = ""
	if i > 0 {
		pwd = uri[0:i]
		url = uri[i+1:]
	} else {
		url = uri
	}
	i = strings.IndexAny(url, "/")
	if i > 0 {
		databaseString := url[i+1:]
		intv, err := strconv.Atoi(databaseString)
		if err != nil {
			database = -1
		}
		database = intv
		url = url[0:i]
	} else {
		database = 0
	}
	i = strings.IndexAny(url, "(")
	if i > 0 {
		network = url[0:i]
		url = url[i+1 : len(url)-1]
	} else {
		network = "tcp"
	}
	return
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
	pwd, network, uri, database := GetUriParam(*This.Uri)
	if database < 0 {
		This.err = fmt.Errorf("database must be in 0 and 16")
		return false
	}
	if network != "tcp" {
		This.err = fmt.Errorf("network must be tcp")
		return false
	}

	universalClient := redis.NewUniversalClient(&redis.UniversalOptions{
		Addrs:    strings.SplitN(uri, ",", -1),
		Password: pwd,
		DB:       database,
		PoolSize: 4096,
	})

	_, This.err = universalClient.Ping(ctx).Result()
	if This.err != nil {
		This.status = ""
		return false
	}
	This.conn = universalClient
	if This.conn == nil {
		This.status = ""
		This.err = errors.New("connect error")
		return false
	} else {
		This.status = "running"
		This.err = nil
		return true
	}
}

func (This *Conn) ReConnect() bool {
	defer func() {
		if err := recover(); err != nil {
			This.err = fmt.Errorf(fmt.Sprint(err))
		}
	}()
	if This.conn != nil {
		This.conn.Close()
	}
	This.Connect()
	return true
}

func (This *Conn) Close() bool {
	if This.conn != nil {
		This.conn.Close()
	}
	return true
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
	var err error
	switch This.p.Type {
	case "set":
		if This.p.ValConfig != "" {
			err = This.conn.Set(ctx, Key, This.getVal(data, index), time.Duration(This.p.Expir)*time.Second).Err()
		} else {
			vbyte, _ := json.Marshal(data.Rows[index])
			err = This.conn.Set(ctx, Key, string(vbyte), time.Duration(This.p.Expir)*time.Second).Err()
		}
		break
	case "list":
		return This.SendToList(Key, data)
		break
	default:
		err = fmt.Errorf(This.p.Type + " not in(set,list)")
		break
	}

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
	switch This.p.Type {
	case "set":
		err = This.conn.Del(ctx, Key).Err()
		break
	case "list":
		return This.SendToList(Key, data)
		break
	default:
		err = fmt.Errorf(This.p.Type + " not in(set,list)")
	}
	if err != nil {
		This.err = err
		return nil, data, err
	}
	return nil, nil, nil
}

func (This *Conn) SendToList(Key string, data *driver.PluginDataType) (*driver.PluginDataType, *driver.PluginDataType, error) {
	var Val string
	var err error
	if This.p.ValConfig != "" {
		Val = This.getVal(data, 0)
	} else {
		c, err := json.Marshal(data)
		if err != nil {
			return nil, data, err
		}
		Val = string(c)
	}
	err = This.conn.LPush(ctx, Key, Val).Err()

	if err != nil {
		return nil, data, err
	}
	return nil, nil, nil
}

func (This *Conn) Query(data *driver.PluginDataType, retry bool) (*driver.PluginDataType, *driver.PluginDataType, error) {
	if This.p.BifrostFilterQuery {
		return nil, nil, nil
	}
	if This.p.Type == "list" {
		Key := This.getKeyVal(data, 0)
		return This.SendToList(Key, data)
	}
	return nil, nil, nil
}

func (This *Conn) Commit(data *driver.PluginDataType, retry bool) (LastSuccessCommitData *driver.PluginDataType, ErrData *driver.PluginDataType, err error) {
	if This.p.BifrostFilterQuery {
		return data, nil, nil
	}
	if This.p.Type == "list" {
		Key := This.getKeyVal(data, 0)
		LastSuccessCommitData, ErrData, err = This.SendToList(Key, data)
		if err != nil {
			return
		}
	}
	return data, nil, nil
}
