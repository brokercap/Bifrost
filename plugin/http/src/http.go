package src

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	pluginDriver "github.com/brokercap/Bifrost/plugin/driver"
)

const VERSION = "v1.8.5"
const BIFROST_VERION = "v1.8.5"

func init() {
	pluginDriver.Register("http", NewConn, VERSION, BIFROST_VERION)
}

type Conn struct {
	pluginDriver.PluginDriverInterface
	uri    *string
	url    string // 解析过后的 http url，不是传参进来的  uri
	user   string
	pwd    string
	status string
	err    error
	p      *PluginParam
}

type HttpContentType string

const (
	HTTP_CONTENT_TYPE_JSON_RAW HttpContentType = "application/json-raw"
)

type PluginParam struct {
	Timeout            int
	ContentType        HttpContentType
	BifrostFilterQuery bool // bifrost server 保留,是否过滤sql事件
}

func NewConn() pluginDriver.Driver {
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
	This.user, This.pwd, This.url = GetUriParam(*This.uri)
	return nil
}

func (This *Conn) GetUriExample() string {
	return "user:pwd@http://a.Bifrist.com?bifrost_api=ok ; http://a.Bifrist.com?bifrost_api=ok"
}

func (This *Conn) CheckUri() error {
	user, pwd, url := GetUriParam(*This.uri)
	client := &http.Client{Timeout: 5 * time.Second}
	req, err := http.NewRequest("GET", url, nil)
	if user != "" {
		req.SetBasicAuth(user, pwd)
	}
	if err != nil {
		return err
	}
	resp, err2 := client.Do(req)
	if err2 != nil {
		return err2
	}
	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		resp.Body.Close()
		return nil
	}
	resp.Body.Close()
	return fmt.Errorf("http code:%d", resp.StatusCode)
}

func GetUriParam(uri string) (string, string, string) {
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
		url = uri[i+1:]
	} else {
		url = uri
	}
	return user, pwd, url
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
	if param.Timeout == 0 {
		param.Timeout = 10
	}
	if param.ContentType != HTTP_CONTENT_TYPE_JSON_RAW {
		return nil, fmt.Errorf("only support application/json(raw)")
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

func (This *Conn) httpPost(data *pluginDriver.PluginDataType) error {
	var req *http.Request
	var client *http.Client
	var err error
	switch This.p.ContentType {
	case HTTP_CONTENT_TYPE_JSON_RAW:
		c, err := json.Marshal(data)
		if err != nil {
			return err
		}
		body := strings.NewReader("\n" + string(c))
		req, err = http.NewRequest("POST", This.url, body)
		req.Header.Set("Content-Type", "application/json")
		break
	default:
		return fmt.Errorf("only support application/json(raw)")
	}
	if err != nil {
		return err
	}
	client = &http.Client{Timeout: time.Duration(This.p.Timeout) * time.Second}
	if This.user != "" {
		req.SetBasicAuth(This.user, This.pwd)
	}
	var resp *http.Response
	resp, err = client.Do(req)
	if err != nil {
		return err
	}
	if resp.StatusCode == 200 || (resp.StatusCode > 200 && resp.StatusCode < 300) {
		resp.Body.Close()
		return nil
	}
	resp.Body.Close()
	return fmt.Errorf("http code:%d", resp.StatusCode)
}

func (This *Conn) Close() bool {
	return true
}

func (This *Conn) Insert(data *pluginDriver.PluginDataType, retry bool) (*pluginDriver.PluginDataType, *pluginDriver.PluginDataType, error) {
	err := This.httpPost(data)
	if err != nil {
		return nil, data, err
	}
	return nil, nil, nil
}

func (This *Conn) Update(data *pluginDriver.PluginDataType, retry bool) (*pluginDriver.PluginDataType, *pluginDriver.PluginDataType, error) {
	err := This.httpPost(data)
	if err != nil {
		return nil, data, err
	}
	return nil, nil, nil
}

func (This *Conn) Del(data *pluginDriver.PluginDataType, retry bool) (*pluginDriver.PluginDataType, *pluginDriver.PluginDataType, error) {
	err := This.httpPost(data)
	if err != nil {
		return nil, data, err
	}
	return nil, nil, nil
}

func (This *Conn) Query(data *pluginDriver.PluginDataType, retry bool) (*pluginDriver.PluginDataType, *pluginDriver.PluginDataType, error) {
	if This.p.BifrostFilterQuery {
		return data, nil, nil
	}
	err := This.httpPost(data)
	if err != nil {
		This.err = err
		return nil, data, err
	}
	return nil, nil, nil
}

func (This *Conn) Commit(data *pluginDriver.PluginDataType, retry bool) (*pluginDriver.PluginDataType, *pluginDriver.PluginDataType, error) {
	if This.p.BifrostFilterQuery {
		return data, nil, nil
	}
	err := This.httpPost(data)
	if err != nil {
		This.err = err
		return nil, data, err
	}
	return data, nil, nil
}
