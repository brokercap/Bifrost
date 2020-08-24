package src

import (
	pluginDriver "github.com/brokercap/Bifrost/plugin/driver"
	"fmt"
	"encoding/json"
	"net/http"
	"strings"
	"time"
	"bytes"
	"net/url"
)


const VERSION  = "v1.3.2"
const BIFROST_VERION = "v1.3.2"

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


func (This *Conn) SetParam(p interface{}) (interface{},error){
	return nil,nil
}

func (This *Conn) httpPost(EventType string,SchemaName string,TableName string,data string) error {
	form := url.Values{
		"SchemaName": {SchemaName},
		"TableName":  {TableName},
		"EventType":  {EventType},
		"Data":     {data},
	}
	body := bytes.NewBufferString(form.Encode())
	//pstring := "EventType="+EventType+"&SchemaName="+SchemaName+"&TableName="+TableName+"&data="+data
	client := &http.Client{Timeout:10 * time.Second}
	req, err := http.NewRequest("POST", This.uri, body)
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	if This.user != ""{
		req.SetBasicAuth(This.user,This.pwd)
	}
	resp, err2 := client.Do(req)
	if err2 != nil {
		resp.Body.Close()
		return err2
	}
	if resp.StatusCode >= 200 && resp.StatusCode<300{
		resp.Body.Close()
		return nil
	}
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
	b,_ := json.Marshal(data.Rows[0])
	err := This.httpPost(data.EventType,data.SchemaName,data.TableName,string(b))
	if err != nil{
		return nil,err
	}
	return &pluginDriver.PluginBinlog{data.BinlogFileNum,data.BinlogPosition},nil
}

func (This *Conn) Update(data *pluginDriver.PluginDataType) (*pluginDriver.PluginBinlog,error) {
	b,_ := json.Marshal(data.Rows)
	err := This.httpPost(data.EventType,data.SchemaName,data.TableName,string(b))
	if err != nil{
		return nil,err
	}
	return &pluginDriver.PluginBinlog{data.BinlogFileNum,data.BinlogPosition},nil
}

func (This *Conn) Del(data *pluginDriver.PluginDataType) (*pluginDriver.PluginBinlog,error) {
	b,_ := json.Marshal(data.Rows[0])
	err := This.httpPost(data.EventType,data.SchemaName,data.TableName,string(b))
	if err != nil{
		return nil,err
	}
	return &pluginDriver.PluginBinlog{data.BinlogFileNum,data.BinlogPosition},nil
}

func (This *Conn) Query(data *pluginDriver.PluginDataType) (*pluginDriver.PluginBinlog,error) {
	err := This.httpPost(data.EventType,data.SchemaName,data.TableName,data.Query)
	if err != nil{
		This.err = err
		return nil,err
	}
	return &pluginDriver.PluginBinlog{data.BinlogFileNum,data.BinlogPosition},nil
}

func (This *Conn) Commit() (*pluginDriver.PluginBinlog,error){
	return nil,nil
}