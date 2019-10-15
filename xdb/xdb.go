package xdb

import (
	"github.com/brokercap/Bifrost/xdb/driver"
	"encoding/json"
)

import (
	_ "github.com/brokercap/Bifrost/xdb/leveldb"
)

const PREFIX  = "xdb"

type Client struct {
	client driver.XdbDriver
}

func NewClient(name ,uri string) (*Client,error){
	client,err := driver.Open(name,uri)
	if err != nil{
		return nil,err
	}
	return &Client{
		client:client,
	},nil
}

func (This *Client) GetKeyVal(table,key string,data interface{}) ([]byte,error){
	myKey := []byte(PREFIX+"-"+table+"-"+key)
	s,err := This.client.GetKeyVal(myKey)
	if err != nil{
		return nil,err
	}
	err2 := json.Unmarshal(s,data)
	if err2 != nil{
		return nil,err2
	}
	return s,err
}

func (This *Client) PutKeyVal(table,key string,data interface{}) error{
	myKey := []byte(PREFIX+"-"+table+"-"+key)
	val,err := json.Marshal(data)
	if err != nil{
		return err
	}
	err = This.client.PutKeyVal(myKey, val)
	return err
}

func (This *Client) GetKeyValBytes(table,key string) ([]byte,error){
	myKey := []byte(PREFIX+"-"+table+"-"+key)
	s,err := This.client.GetKeyVal(myKey)
	return s,err
}

func (This *Client) PutKeyValBytes(table,key string,val []byte) error{
	myKey := []byte(PREFIX+"-"+table+"-"+key)
	err := This.client.PutKeyVal(myKey, val)
	return err
}


func (This *Client) DelKeyVal(table,key string) error{
	myKey := []byte(PREFIX+"-"+table+"-"+key)
	return This.client.DelKeyVal(myKey)
}

func (This *Client) GetListByKeyPrefix(table,key string,data interface{}) ([]string,error){
	myKey := []byte(PREFIX+"-"+table+"-"+key)
	s,err := This.client.GetListByKeyPrefix(myKey)
	if err != nil{
		return s,err
	}
	val := ""
	for _,v := range s{
		if val == ""{
			val = v
		}else{
			val += ","+v
		}
	}
	val = "["+val+"]"
	err2 := json.Unmarshal([]byte(val),&data)
	return s,err2
}


func (This *Client) Close() error{
	return This.client.Close()
}
