package src

import (
	pluginDriver "github.com/brokercap/Bifrost/plugin/driver"
	"fmt"
	"encoding/json"
	"net/http"
	"strings"
	"time"
)


const VERSION  = "v1.4.1"
const BIFROST_VERION = "v1.4.1"

func init(){
	pluginDriver.Register("http",&MyConn{},VERSION,BIFROST_VERION)
}

type MyConn struct {}

func (MyConn *MyConn) Open(uri string) pluginDriver.ConnFun{
	return newConn(uri)
}

func (MyConn *MyConn) GetUriExample() string{
	return "user:pwd@http://a.Bifrist.com?bifrost_api=ok ; http://a.Bifrist.com?bifrost_api=ok"
}

func (MyConn *MyConn) CheckUri(uri string) error{
	user,pwd,url := getUriParam(uri)
	client := &http.Client{Timeout:5 * time.Second}
	req, err := http.NewRequest("GET", url,nil)
	if user != ""{
		req.SetBasicAuth(user,pwd)
	}
	if err != nil {
		return err
	}
	resp, err2 := client.Do(req)
	if err2 != nil {
		return err2
	}
	if resp.StatusCode >= 200 && resp.StatusCode<300{
		resp.Body.Close()
		return nil
	}
	resp.Body.Close()
	return fmt.Errorf("http code:%d",resp.StatusCode)
}

type Conn struct {
	uri    	string
	user 	string
	pwd 	string
	status  string
	err     error
	p		*PluginParam
}

type HttpContentType string

const (
	HTTP_CONTENT_TYPE_JSON_RAW 	HttpContentType = "application/json-raw"
)


type PluginParam struct {
	Timeout	int
	ContentType HttpContentType
}

func getUriParam(uri string)(string,string,string){
	i := strings.IndexAny(uri, "@")
	var user , pwd string = "",""
	var url string
	if i > 0{
		t := uri[0:i]
		j := strings.IndexAny(t, ":")
		if j > 0{
			user = t[0:j]
			pwd = t[j+1:]
		}else{
			user = t
		}
		url = uri[0:i]
	}else{
		url = uri[i+1:]
	}
	return user,pwd,url
}

func newConn(uri string) *Conn{
	user,pwd,url := getUriParam(uri)
	f := &Conn{
		uri:url,
		user:user,
		pwd:pwd,
	}
	return f
}


func (This *Conn) GetConnStatus() string {
	return This.status
}

func (This *Conn) SetConnStatus(status string) {
	This.status = status
}

func (This *Conn) GetParam(p interface{}) (*PluginParam,error){
	s,err := json.Marshal(p)
	if err != nil{
		return nil,err
	}
	var param PluginParam
	err2 := json.Unmarshal(s,&param)
	if err2 != nil{
		return nil,err2
	}
	if param.Timeout == 0 {
		param.Timeout = 10
	}
	if param.ContentType != HTTP_CONTENT_TYPE_JSON_RAW {
		return nil,fmt.Errorf("only support application/json(raw)")
	}
	This.p = &param
	return &param,nil
}


func (This *Conn) SetParam(p interface{}) (interface{},error){
	if p == nil{
		return nil,fmt.Errorf("param is nil")
	}
	switch p.(type) {
	case *PluginParam:
		This.p = p.(*PluginParam)
		return p,nil
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
		c,_:=json.Marshal(data)
		body := strings.NewReader("\n"+string(c))
		req, err = http.NewRequest("POST", This.uri, body)
		req.Header.Set("Content-Type", "application/json")
		break
	default:
		return fmt.Errorf("only support application/json(raw)")
	}
	if err != nil {
		return err
	}
	client = &http.Client{Timeout:time.Duration(This.p.Timeout) * time.Second}
	if This.user != ""{
		req.SetBasicAuth(This.user,This.pwd)
	}
	var resp *http.Response
	resp, err = client.Do(req)
	if err != nil {
		return err
	}
	if resp.StatusCode == 200 || (resp.StatusCode > 200 && resp.StatusCode<300) {
		resp.Body.Close()
		return nil
	}
	resp.Body.Close()
	return fmt.Errorf("http code:%d",resp.StatusCode)
}


func (This *Conn) Connect() bool {
	return true
}

func (This *Conn) ReConnect() bool {
	return  true
}

func (This *Conn) HeartCheck() {
	return
}

func (This *Conn) Close() bool {
	return true
}

func (This *Conn) Insert(data *pluginDriver.PluginDataType) (*pluginDriver.PluginBinlog,error) {
	err := This.httpPost(data)
	if err != nil{
		return nil,err
	}
	return &pluginDriver.PluginBinlog{data.BinlogFileNum,data.BinlogPosition},nil
}

func (This *Conn) Update(data *pluginDriver.PluginDataType) (*pluginDriver.PluginBinlog,error) {
	err := This.httpPost(data)
	if err != nil{
		return nil,err
	}
	return &pluginDriver.PluginBinlog{data.BinlogFileNum,data.BinlogPosition},nil
}

func (This *Conn) Del(data *pluginDriver.PluginDataType) (*pluginDriver.PluginBinlog,error) {
	err := This.httpPost(data)
	if err != nil{
		return nil,err
	}
	return &pluginDriver.PluginBinlog{data.BinlogFileNum,data.BinlogPosition},nil
}

func (This *Conn) Query(data *pluginDriver.PluginDataType) (*pluginDriver.PluginBinlog,error) {
	err := This.httpPost(data)
	if err != nil{
		This.err = err
		return nil,err
	}
	return &pluginDriver.PluginBinlog{data.BinlogFileNum,data.BinlogPosition},nil
}

func (This *Conn) Commit() (*pluginDriver.PluginBinlog,error){
	return nil,nil
}