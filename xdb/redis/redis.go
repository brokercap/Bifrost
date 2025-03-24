package redis

import (
	"context"
	"fmt"
	"github.com/brokercap/Bifrost/xdb/driver"
	"github.com/go-redis/redis/v8"
	"strconv"
	"strings"
	"time"
)

const VERSION = "v1.1.1"

type MyConn struct{}

var ctx = context.Background()

func (MyConn *MyConn) Open(uri string) (driver.XdbDriver, error) {
	return newConn(uri)
}

func newConn(uri string) (*Conn, error) {
	pwd, network, uri, database := getUriParam(uri)
	f := &Conn{
		pwd:      pwd,
		network:  network,
		database: database,
		Uri:      uri,
	}
	f.Connect()
	return f, nil
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
}

func (This *Conn) Connect() error {
	if This.database < 0 || This.database > 16 {
		This.err = fmt.Errorf("database must be in 0 and 16")
		return This.err
	}
	if This.network != "tcp" {
		This.err = fmt.Errorf("network must be tcp")
		return This.err
	}
	universalClient := redis.NewUniversalClient(&redis.UniversalOptions{
		Addrs:    strings.SplitN(This.Uri, ",", -1),
		Password: This.pwd,
		DB:       This.database,
		PoolSize: 4096,
	})

	_, This.err = universalClient.Ping(ctx).Result()
	if This.err != nil {
		This.status = ""
		return This.err
	}

	This.conn = universalClient

	if This.conn == nil {
		This.err = fmt.Errorf("redis connect error")
		This.status = ""
		return This.err
	} else {
		This.err = nil
		return This.err
	}
}

func (This *Conn) Close() error {
	if This.conn != nil {
		This.conn.Close()
	}
	This.conn = nil
	return nil
}

func (This *Conn) InitConn() {
	if This.conn == nil {
		This.Connect()
	}
}

func (This *Conn) GetKeyVal(key []byte) ([]byte, error) {
	This.InitConn()
	f := This.conn.Get(ctx, string(key))
	s, err := f.Bytes()
	if err != nil {
		if err.Error() == "redis: nil" {
			return nil, nil
		}
		This.Close()
		return nil, err
	}
	return s, nil
}

func (This *Conn) PutKeyVal(key []byte, val []byte) error {
	This.InitConn()
	err := This.conn.Set(ctx, string(key), string(val), time.Duration(0)).Err()
	if err != nil {
		This.Close()
		return err
	}
	return nil
}

func (This *Conn) DelKeyVal(key []byte) error {
	This.InitConn()
	err := This.conn.Del(ctx, string(key)).Err()
	if err != nil {
		if err.Error() != "redis: nil" {
			This.Close()
			return err
		}
	}
	return nil
}

func (This *Conn) GetListByKeyPrefix(key []byte) ([]driver.ListValue, error) {
	This.InitConn()
	data := make([]driver.ListValue, 0)
	list, err := This.conn.Keys(ctx, string(key)+"*").Result()
	if err != nil {
		This.Close()
		return data, err
	}
	for _, vk := range list {
		val, err := This.GetKeyVal([]byte(vk))
		if err != nil {
			return data, err
		}
		data = append(data,
			driver.ListValue{
				Key:   vk,
				Value: string(val),
			})
	}
	return data, nil
}
