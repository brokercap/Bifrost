package src

import (
	"encoding/json"
	"fmt"
	"github.com/brokercap/Bifrost/plugin/driver"
	"github.com/go-redis/redis"
	"strconv"
	"strings"
	"time"
)

const VERSION = "v1.4.3"
const BIFROST_VERION = "v1.4.4"

func init() {
	driver.Register("redis", &MyConn{}, VERSION, BIFROST_VERION)
}

type MyConn struct{}

func (MyConn *MyConn) Open(uri string) driver.ConnFun {
	return newConn(uri)
}

func (MyConn *MyConn) GetUriExample() string {
	return "pwd@tcp(127.0.0.1:6379)/0 or 127.0.0.1:6379 or pwd@tcp(127.0.0.1:6379,127.0.0.1:6380)/0 or 127.0.0.1:6379,127.0.0.1:6380"
}

func (MyConn *MyConn) CheckUri(uri string) error {
	c := newConn(uri)
	if c.err != nil {
		return c.err
	}
	c.Close()
	return nil
}

func getUriParam(uri string) (pwd string, network string, url string, database int) {
	i := strings.IndexAny(uri, "@")
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

type Conn struct {
	Uri      string
	pwd      string
	database int
	network  string
	status   string
	conn     redis.UniversalClient
	err      error
	p        *PluginParam
}

type PluginParam struct {
	KeyConfig      string
	FieldKeyConfig string
	SortedConfig   string
	Type           string
	Expir          int
}

func newConn(uri string) *Conn {
	pwd, network, uri, database := getUriParam(uri)
	f := &Conn{
		pwd:      pwd,
		network:  network,
		database: database,
		Uri:      uri,
	}
	f.Connect()
	return f
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
		s, err := json.Marshal(p)
		if err != nil {
			return nil, err
		}
		var param PluginParam
		err = json.Unmarshal(s, &param)
		if err != nil {
			return nil, err
		}
		This.p = &param
		return &param, nil
	}
}

func (This *Conn) GetConnStatus() string {
	return This.status
}

func (This *Conn) SetConnStatus(status string) {
	This.status = status
}

func (This *Conn) Connect() bool {
	if This.database < 0 || This.database > 16 {
		This.err = fmt.Errorf("database must be in 0 and 16")
		return false
	}
	if This.network != "tcp" {
		This.err = fmt.Errorf("network must be tcp")
		return false
	}

	universalClient := redis.NewUniversalClient(&redis.UniversalOptions{
		Addrs:    strings.SplitN(This.Uri, ",", -1),
		Password: This.pwd,
		DB:       This.database,
		PoolSize: 4096,
	})

	_, This.err = universalClient.Ping().Result()
	if This.err != nil {
		This.status = ""
		return false
	}
	This.conn = universalClient
	if This.conn == nil {
		This.status = ""
		return false
	} else {
		This.err = nil
		return true
	}
}

func (This *Conn) ReConnect() bool {
	result := true
	defer func() {
		if err := recover(); err != nil {
			This.err = fmt.Errorf(fmt.Sprint(err))
			result = false
		}
	}()
	This.conn.Close()
	This.Connect()
	return result
}

func (This *Conn) HeartCheck() {
	return
}

func (This *Conn) Close() bool {
	err := This.conn.Close()
	if err != nil {
		return false
	}
	return true
}

func (This *Conn) Insert(data *driver.PluginDataType) (*driver.PluginBinlog, error) {
	return This.Update(data)
}

func (This *Conn) Update(data *driver.PluginDataType) (*driver.PluginBinlog, error) {
	if This.err != nil {
		This.ReConnect()
	}

	var err error

	index := len(data.Rows) - 1
	key := This.getKeyVal(data, This.p.KeyConfig, index)

	j, err := json.Marshal(data.Rows[index])
	if err != nil {
		return nil, err
	}

	switch This.p.Type {
	case "string":
		{
			pipeline := This.conn.Pipeline()
			if len(data.Rows) >= 2 {
				oldKey := This.getKeyVal(data, This.p.FieldKeyConfig, 0)
				pipeline.Del(oldKey)
			}
			pipeline.Set(key, string(j), time.Duration(This.p.Expir)*time.Second)
			_, err = pipeline.Exec()
		}
	case "hash":
		{
			pipeline := This.conn.Pipeline()
			if len(data.Rows) >= 2 {
				oldKey := This.getKeyVal(data, This.p.FieldKeyConfig, 0)
				oldFiledKey := This.getKeyVal(data, This.p.FieldKeyConfig, 0)
				pipeline.HDel(oldKey, oldFiledKey)
			}
			fieldKey := This.getKeyVal(data, This.p.FieldKeyConfig, index)
			pipeline.HSet(key, fieldKey, string(j))
			_, err = pipeline.Exec()
		}
	case "zset":
		{
			sort, err := strconv.ParseFloat(This.getKeyVal(data, This.p.SortedConfig, index), 64)
			if err != nil {
				sort = 0
			}

			pipeline := This.conn.Pipeline()
			if len(data.Rows) >= 2 {
				oldKey := This.getKeyVal(data, This.p.FieldKeyConfig, 0)
				if jo, err := json.Marshal(data.Rows[0]); err == nil {
					pipeline.ZRem(oldKey, 1, string(jo))
				}
			}
			pipeline.ZAdd(key, redis.Z{Score: sort, Member: string(j)})
			_, err = pipeline.Exec()
		}
	case "list":
		{
			pipeline := This.conn.Pipeline()
			if len(data.Rows) >= 2 {
				if jo, err := json.Marshal(data.Rows[0]); err == nil {
					oldKey := This.getKeyVal(data, This.p.FieldKeyConfig, 0)
					pipeline.LRem(oldKey, 1, string(jo))
				}
			}
			pipeline.LPush(key, string(j))
			_, err = pipeline.Exec()
		}
	case "set":
		{
			pipeline := This.conn.Pipeline()
			if len(data.Rows) >= 2 {
				if jo, err := json.Marshal(data.Rows[0]); err == nil {
					oldKey := This.getKeyVal(data, This.p.FieldKeyConfig, 0)
					pipeline.SRem(oldKey, 1, string(jo))
				}
			}
			pipeline.SAdd(key, string(j))
			_, err = pipeline.Exec()
		}
	default:
		err = fmt.Errorf(This.p.Type + " not in(string,set,hash,list)")
	}

	if err != nil {
		This.err = err
		return nil, err
	}
	return &driver.PluginBinlog{BinlogFileNum: data.BinlogFileNum, BinlogPosition: data.BinlogPosition}, nil
}

func (This *Conn) Del(data *driver.PluginDataType) (*driver.PluginBinlog, error) {
	if This.err != nil {
		This.ReConnect()
	}
	key := This.getKeyVal(data, This.p.KeyConfig, 0)
	var err error
	switch This.p.Type {
	case "string":
		err = This.conn.Del(key).Err()
	case "hash":
		fieldKey := This.getKeyVal(data, This.p.FieldKeyConfig, 0)
		err = This.conn.HDel(key, fieldKey).Err()
	case "zset":
		j, e := json.Marshal(data.Rows[0])
		if e != nil {
			return nil, e
		}
		err = This.conn.ZRem(key, string(j)).Err()
	case "list":
		j, e := json.Marshal(data.Rows[0])
		if e != nil {
			return nil, e
		}
		err = This.conn.LRem(key, 1, string(j)).Err()
	case "set":
		j, e := json.Marshal(data.Rows[0])
		if e != nil {
			return nil, e
		}
		err = This.conn.SRem(key, string(j)).Err()
	default:
		err = fmt.Errorf(This.p.Type + " not in(string,set,hash,list)")
	}
	if err != nil {
		This.err = err
		return nil, err
	}
	return &driver.PluginBinlog{BinlogFileNum: data.BinlogFileNum, BinlogPosition: data.BinlogPosition}, nil
}

func (This *Conn) Query(data *driver.PluginDataType) (*driver.PluginBinlog, error) {
	return &driver.PluginBinlog{BinlogFileNum: data.BinlogFileNum, BinlogPosition: data.BinlogPosition}, nil
}

func (This *Conn) Commit() (*driver.PluginBinlog, error) {
	return nil, nil
}

func (This *Conn) getKeyVal(data *driver.PluginDataType, key string, index int) string {
	return fmt.Sprint(driver.TransfeResult(key, data, index))
}
