package src

import (
	pluginDriver "github.com/jc3wish/Bifrost/plugin/driver"
	"fmt"
	"encoding/json"
	"net/http"
	"strings"
	"log"
	"time"
)


const VERSION  = "v1.1.0"
const BIFROST_VERION = "v1.1.0"

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
		log.Println(user,pwd)
		req.SetBasicAuth(user,pwd)
	}
	if err != nil {
		return err
	}
	resp, err2 := client.Do(req)
	if err2 != nil {
		return err2
	}
	if resp.StatusCode >= 200 || resp.StatusCode<300{
		resp.Body.Close()
		return nil
	}
	resp.Body.Close()
	return fmt.Errorf("http code:%s",resp.StatusCode)
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


func (This *Conn) SetParam(p interface{}) error{
	return nil
}

func (This *Conn) httpPost(EventType string,SchemaName string,TableName string,data string) error {
	pstring := "EventType="+EventType+"&SchemaName="+SchemaName+"&TableName="+TableName+"&data="+data
	client := &http.Client{}
	req, err := http.NewRequest("POST", This.uri, strings.NewReader(pstring))
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
	if resp.StatusCode >= 200 || resp.StatusCode<300{
		resp.Body.Close()
		return nil
	}
	return fmt.Errorf("http code:%s",resp.StatusCode)
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

func (This *Conn) Insert(data *pluginDriver.PluginDataType) (bool,error) {
	b,_ := json.Marshal(data.Rows[0])
	err := This.httpPost(data.EventType,data.SchemaName,data.TableName,string(b))
	if err != nil{
		return false,err
	}
	return true,nil
}

func (This *Conn) Update(data *pluginDriver.PluginDataType) (bool,error) {
	b,_ := json.Marshal(data.Rows)
	err := This.httpPost(data.EventType,data.SchemaName,data.TableName,string(b))
	if err != nil{
		return false,err
	}
	return true,nil
}

func (This *Conn) Del(data *pluginDriver.PluginDataType) (bool,error) {
	b,_ := json.Marshal(data.Rows[0])
	err := This.httpPost(data.EventType,data.SchemaName,data.TableName,string(b))
	if err != nil{
		return false,err
	}
	return true,nil
}

func (This *Conn) Query(data *pluginDriver.PluginDataType) (bool,error) {
	err := This.httpPost(data.EventType,data.SchemaName,data.TableName,data.Query)
	if err != nil{
		This.err = err
		return false,err
	}
	return true,nil
}
