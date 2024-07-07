package xdb

import (
	"encoding/json"
	"github.com/brokercap/Bifrost/xdb/driver"
)

import (
	_ "github.com/brokercap/Bifrost/xdb/leveldb"
	_ "github.com/brokercap/Bifrost/xdb/redis"
)

const DEFAULT_PREFIX = "xdb"

type Client struct {
	prefix string
	client driver.XdbDriver
}

func NewClient(name, uri string) (*Client, error) {
	client, err := driver.Open(name, uri)
	if err != nil {
		return nil, err
	}
	return &Client{
		client: client,
		prefix: DEFAULT_PREFIX,
	}, nil
}

func (This *Client) SetPrefix(prefix string) *Client {
	This.prefix = prefix
	return This
}

func (This *Client) GetKeyVal(table, key string, data interface{}) ([]byte, error) {
	myKey := []byte(This.prefix + "-" + table + "-" + key)
	s, err := This.client.GetKeyVal(myKey)
	if err != nil {
		return nil, err
	}
	err2 := json.Unmarshal(s, data)
	if err2 != nil {
		return nil, err2
	}
	return s, err
}

func (This *Client) PutKeyVal(table, key string, data interface{}) error {
	myKey := []byte(This.prefix + "-" + table + "-" + key)
	val, err := json.Marshal(data)
	if err != nil {
		return err
	}
	err = This.client.PutKeyVal(myKey, val)
	return err
}

func (This *Client) GetKeyValBytes(table, key string) ([]byte, error) {
	myKey := []byte(This.prefix + "-" + table + "-" + key)
	s, err := This.client.GetKeyVal(myKey)
	return s, err
}

func (This *Client) PutKeyValBytes(table, key string, val []byte) error {
	myKey := []byte(This.prefix + "-" + table + "-" + key)
	err := This.client.PutKeyVal(myKey, val)
	return err
}

func (This *Client) DelKeyVal(table, key string) error {
	myKey := []byte(This.prefix + "-" + table + "-" + key)
	return This.client.DelKeyVal(myKey)
}

func (This *Client) GetListByKeyPrefix(table, key string, data interface{}) ([]driver.ListValue, error) {
	prefix := This.prefix + "-" + table + "-"
	prefixLen := len(prefix)
	myKey := []byte(prefix + key)
	s, err := This.client.GetListByKeyPrefix(myKey)
	if err != nil {
		return s, err
	}
	var val = ""
	for k, v := range s {
		if data != nil {
			if val == "" {
				val = v.Value
			} else {
				val += "," + v.Value
			}
		}
		s[k].Key = s[k].Key[prefixLen:]
	}

	if data != nil {
		val = "[" + val + "]"
		err = json.Unmarshal([]byte(val), &data)
	}
	return s, err
}

func (This *Client) Close() error {
	return This.client.Close()
}
