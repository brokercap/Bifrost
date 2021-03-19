package src

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/brokercap/Bifrost/plugin/driver"
	"time"

	//"github.com/go-redis/redis"
	"github.com/go-redis/redis/v8"
	"strconv"
	"strings"
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
	KeyConfig string
	DataType  string
	ValConfig string
	Type      string

	HashKey string
	Sort    string
	Expired int

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

func (This *Conn) getTemplateVal(data *driver.PluginDataType, template string, index int) string {
	return fmt.Sprint(driver.TransfeResult(template, data, index))
}

func (This *Conn) getRedisContent(data *driver.PluginDataType, index int) (string, error) {
	if This.p.DataType == "custom" {
		return This.getTemplateVal(data, This.p.ValConfig, index), nil
	} else {
		j, err := json.Marshal(data.Rows[index])
		return string(j), err
	}
}

func (This *Conn) Insert(data *driver.PluginDataType, retry bool) (*driver.PluginDataType, *driver.PluginDataType, error) {
	return This.Update(data, retry)
}

func (This *Conn) Update(data *driver.PluginDataType, retry bool) (*driver.PluginDataType, *driver.PluginDataType, error) {
	if This.err != nil {
		This.ReConnect()
	}

	var err error
	index := len(data.Rows) - 1
	key := This.getTemplateVal(data, This.p.KeyConfig, index)
	content, err := This.getRedisContent(data, index)
	pipeline := This.conn.Pipeline()
	ctx := context.Background()

	switch This.p.Type {
	case "set":
		{
			//删除之前的内容
			if len(data.Rows) >= 2 {
				oldKey := This.getTemplateVal(data, This.p.KeyConfig, 0)
				pipeline.Del(ctx, oldKey)
			}
			pipeline.Set(ctx, key, content, time.Duration(This.p.Expired)*time.Second)
		}
	case "hash":
		{
			//删除之前的内容
			if len(data.Rows) >= 2 {
				oldKey := This.getTemplateVal(data, This.p.KeyConfig, 0)
				oldHashKey := This.getTemplateVal(data, This.p.HashKey, 0)
				pipeline.HDel(ctx, oldKey, oldHashKey)
			}
			hashKey := This.getTemplateVal(data, This.p.HashKey, index)
			pipeline.HSet(ctx, key, hashKey, content)
		}
	case "zset":
		{
			//如果sort 字段无法转换为 数字 默认使用0
			sort, sortErr := strconv.ParseFloat(This.getTemplateVal(data, This.p.Sort, index), 64)
			if sortErr != nil {
				sort = 0
			}
			//删除之前的内容
			if len(data.Rows) >= 2 {
				oldKey := This.getTemplateVal(data, This.p.KeyConfig, 0)
				if oldContent, err := This.getRedisContent(data, 0); err == nil {
					pipeline.ZRem(ctx, oldKey, 1, oldContent)
				}
			}
			pipeline.ZAdd(ctx, key, &redis.Z{Score: sort, Member: content})
		}
	case "list":
		{
			//删除之前的内容
			if len(data.Rows) >= 2 {
				if oldContent, err := This.getRedisContent(data, 0); err == nil {
					oldKey := This.getTemplateVal(data, This.p.KeyConfig, 0)
					pipeline.LRem(ctx, oldKey, 1, oldContent)
				}
			}
			pipeline.LPush(ctx, key, content)
		}
	case "sadd":
		{
			//删除之前的内容
			if len(data.Rows) >= 2 {
				if oldContent, err := This.getRedisContent(data, 0); err == nil {
					oldKey := This.getTemplateVal(data, This.p.KeyConfig, 0)
					pipeline.SRem(ctx, oldKey, 1, oldContent)
				}
			}
			pipeline.SAdd(ctx, key, content)
		}
	default:
		err = fmt.Errorf(This.p.Type + " not in(string,set,hash,list)")
	}
	if err != nil {
		This.err = err
		return nil, data, err
	}
	_, err = pipeline.Exec(ctx)
	if err != nil {
		This.err = err
		return nil, data, err
	} else {
		return nil, nil, nil
	}
}

func (This *Conn) Del(data *driver.PluginDataType, retry bool) (*driver.PluginDataType, *driver.PluginDataType, error) {
	if This.err != nil {
		This.ReConnect()
	}

	var err error
	key := This.getTemplateVal(data, This.p.KeyConfig, 0)
	ctx := context.Background()

	switch This.p.Type {
	case "set":
		err = This.conn.Del(ctx, key).Err()
	case "hash":
		hashKey := This.getTemplateVal(data, This.p.HashKey, 0)
		err = This.conn.HDel(ctx, key, hashKey).Err()
	case "zset":
		oldContent, err := This.getRedisContent(data, 0)
		if err == nil {
			err = This.conn.ZRem(ctx, key, oldContent).Err()
		}
	case "list":
		oldContent, err := This.getRedisContent(data, 0)
		if err == nil {
			err = This.conn.LRem(ctx, key, 1, oldContent).Err()
		}
	case "sadd":
		oldContent, err := This.getRedisContent(data, 0)
		if err == nil {
			err = This.conn.SRem(ctx, key, oldContent).Err()
		}
	default:
		err = fmt.Errorf(This.p.Type + " not in(string,set,hash,list)")
	}

	if err != nil {
		This.err = err
		return nil, data, err
	} else {
		return nil, nil, nil
	}
}

func (This *Conn) Query(data *driver.PluginDataType, retry bool) (*driver.PluginDataType, *driver.PluginDataType, error) {
	if This.p.BifrostFilterQuery {
		return nil, nil, nil
	}
	if This.p.Type == "list" {
		return This.sendList(data, retry)
	}
	return nil, nil, nil
}

func (This *Conn) Commit(data *driver.PluginDataType, retry bool) (LastSuccessCommitData *driver.PluginDataType, ErrData *driver.PluginDataType, err error) {
	if This.p.BifrostFilterQuery {
		return data, nil, nil
	}
	if This.p.Type == "list" {
		return This.sendList(data, retry)
	}
	return data, nil, nil
}

//老版本兼容
func (This *Conn) sendList(data *driver.PluginDataType, retry bool) (*driver.PluginDataType, *driver.PluginDataType, error) {
	key := This.getTemplateVal(data, This.p.KeyConfig, 0)
	content, err := This.getRedisContent(data, 0)
	if err != nil {
		return nil, nil, err
	}
	if err = This.conn.LPush(context.Background(), key, content).Err(); err != nil {
		return nil, nil, err
	}
	return data, nil, nil
}
