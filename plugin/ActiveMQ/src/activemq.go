package src

import (
	"encoding/json"
	"fmt"
	pluginDriver "github.com/brokercap/Bifrost/plugin/driver"
	"github.com/gmallard/stompngo"
	"log"
	"net"
	"strconv"
	"strings"
	"time"
)

const VERSION = "v1.6.0"
const BIFROST_VERION = "v1.6.0"

func init() {
	pluginDriver.Register("ActiveMQ", NewConn, VERSION, BIFROST_VERION)
}

func NewConn() pluginDriver.Driver {
	return &Conn{status: "close"}
}

type Conn struct {
	pluginDriver.PluginDriverInterface
	Uri    *string
	status string
	conn   *stompngo.Connection
	err    error
	header stompngo.Headers
	p      *PluginParam
}

type PluginParam struct {
	QueueName          string
	Persistent         bool
	Expir              int
	BifrostFilterQuery bool // bifrost server 保留,是否过滤sql事件
}

func (This *Conn) GetUriExample() string {
	return "127.0.0.1:61613"
}

func (This *Conn) SetOption(uri *string, param map[string]interface{}) {
	This.Uri = uri
	return
}

func (This *Conn) CheckUri() (err error) {
	err = fmt.Errorf("unkow error!")
	This.Connect()
	if This.err != nil {
		return This.err
	}
	This.Close()
	return nil
}

func (This *Conn) Open() error {
	This.Connect()
	return nil
}

func (This *Conn) Connect() bool {
	user, pwd, uri := getUriParam(*This.Uri)
	n, e := net.Dial("tcp", uri)
	if e != nil {
		log.Println("net conn err:", e)
		This.err = e
		This.status = "close"
		return false
	}
	if user == "" {
		This.header = stompngo.Headers{}
	} else {
		This.header = stompngo.Headers{stompngo.HK_LOGIN, user, stompngo.HK_PASSCODE, pwd}
	}

	This.conn, e = stompngo.Connect(n, This.header)
	if e != nil {
		This.err = e
		This.status = "close"
		return false
	}
	This.status = "running"
	return true
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
	if param.QueueName == "" {
		return nil, fmt.Errorf("QueueName is empty")
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

func getUriParam(uri string) (string, string, string) {
	i := strings.IndexAny(uri, "@")
	var user, pwd string = "", ""
	var url string
	if i > 0 {
		t := uri[0:i]
		j := strings.IndexAny(t, ":")
		if j > 0 {
			user = t[0:j]
			pwd = t[j+1:]
		} else {
			user = t
		}
		url = uri[0:i]
	} else {
		url = uri[i+1:]
	}
	return user, pwd, url
}

func (This *Conn) ReConnect() bool {
	This.Close()
	r := This.Connect()
	if r == true {
		return true
	} else {
		return false
	}
}

func (This *Conn) Close() bool {
	if This.conn != nil {
		func() {
			defer func() {
				if err := recover(); err != nil {
					return
				}
			}()
			This.conn.Disconnect(This.header)
		}()
	}
	return true
}

func (This *Conn) Insert(data *pluginDriver.PluginDataType, retry bool) (*pluginDriver.PluginDataType, *pluginDriver.PluginDataType, error) {
	return This.sendToList(data)
}

func (This *Conn) Update(data *pluginDriver.PluginDataType, retry bool) (*pluginDriver.PluginDataType, *pluginDriver.PluginDataType, error) {
	return This.sendToList(data)
}

func (This *Conn) Del(data *pluginDriver.PluginDataType, retry bool) (*pluginDriver.PluginDataType, *pluginDriver.PluginDataType, error) {
	return This.sendToList(data)
}

func (This *Conn) Query(data *pluginDriver.PluginDataType, retry bool) (*pluginDriver.PluginDataType, *pluginDriver.PluginDataType, error) {
	return This.sendToList(data)
}

func (This *Conn) Commit(data *pluginDriver.PluginDataType, retry bool) (*pluginDriver.PluginDataType, *pluginDriver.PluginDataType, error) {
	if This.p.BifrostFilterQuery {
		return data, nil, nil
	}
	_, _, err := This.sendToList(data)
	if err == nil {
		return data, nil, nil
	}
	return nil, nil, err
}

func (This *Conn) sendToList(data *pluginDriver.PluginDataType) (LastSuccessCommitData *pluginDriver.PluginDataType, ErrData *pluginDriver.PluginDataType, err error) {
	if This.status != "running" {
		This.ReConnect()
		if This.status != "running" {
			return nil, data, This.err
		}
	}
	c, err := json.Marshal(data)
	if err != nil {
		This.err = err
		return nil, data, err
	}
	QueueName := fmt.Sprint(pluginDriver.TransfeResult(This.p.QueueName, data, len(data.Rows)-1))
	var h stompngo.Headers
	h = h.Add(stompngo.HK_DESTINATION, QueueName)
	if This.p.Persistent == true {
		h = h.Add("persistent", "true")
	}
	if This.p.Expir > 0 {
		h = h.Add("expires", strconv.FormatInt((time.Now().Unix()*1000+int64(This.p.Expir)), 10))
	}
	err2 := This.conn.SendBytes(h, c)
	if err2 != nil {
		This.err = err2
		This.status = "close"
		return nil, data, err
	}
	return nil, nil, nil
}

func (This *Conn) TimeOutCommit() (*pluginDriver.PluginDataType, *pluginDriver.PluginDataType, error) {
	return nil, nil, nil
}

func (This *Conn) Skip(SkipData *pluginDriver.PluginDataType) error {
	return nil
}
