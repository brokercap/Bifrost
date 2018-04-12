package http

import (
	"github.com/Bifrost/toserver/driver"
	"fmt"
	"encoding/json"
	"net/http"
	"strings"
	dataDriver "database/sql/driver"
	"strconv"
)

func init(){
	driver.Register("http",&MyConn{})
}

type MyConn struct {}

func (MyConn *MyConn) Open(uri string) driver.ConnFun{
	return newConn(uri)
}

func (MyConn *MyConn) GetTypeAndRule() driver.TypeAndRule{
	return driver.TypeAndRule{
		DataTypeList:[]string{"json","string"},
		TypeList: map[string]driver.TypeRule{
			"set":driver.TypeRule{
				Key:"(.*)",
				Val:"(.*)",
				KeyExample:"",
			},
		},
	}
}

func (MyConn *MyConn) GetUriExample() string{
	return "user:pwd@http://a.Bifrist.com?myapi=ok ; http://a.Bifrist.com?myapi=ok"
}

func (MyConn *MyConn) CheckUri(uri string) error{
	user,pwd,url := getUriParam(uri)
	client := &http.Client{}
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
	if resp.StatusCode >= 200 || resp.StatusCode<300{
		resp.Body.Close()
		return nil
	}
	resp.Body.Close()
	return fmt.Errorf("http code:%s",resp.StatusCode)
}

type Conn struct {
	Uri    	string
	User 	string
	Pwd 	string
	status  string
	err     error
	expir   int
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
		Uri:url,
		User:user,
		Pwd:pwd,
	}
	return f
}


func (This *Conn) GetConnStatus() string {
	return This.status
}

func (This *Conn) SetConnStatus(status string) {
	This.status = status
}

func (This *Conn) SetExpir(TimeOut int) {
	This.expir = TimeOut
}

func (This *Conn) httpPost(optype string,key string,data string) error {
	pstring := "optype="+optype+"&key="+key+"&data="+data+"&expir="+strconv.Itoa(This.expir)
	client := &http.Client{}
	req, err := http.NewRequest("POST", This.Uri, strings.NewReader(pstring))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	if This.User != ""{
		req.SetBasicAuth(This.User,This.Pwd)
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

func (This *Conn) Insert(key string, data interface{}) (bool,error) {
	var c string
	switch data.(type){
	case string:
		c = data.(string)
		break
	case map[string]interface{}:
		cb, _ := json.Marshal(data.(map[string]interface{}))
		c = string(cb)
		break
	case map[string]dataDriver.Value:
		cb, _ := json.Marshal(data.(map[string]dataDriver.Value))
		c = string(cb)
		break
	default:
		c = fmt.Sprint(data)
		break
	}
	err := This.httpPost("insert",key,c)
	if err != nil{
		return false,err
	}
	return true,nil
}

func (This *Conn) Update(key string, data interface{}) (bool,error) {
	var c string
	switch data.(type){
	case string:
		c = data.(string)
		break
	case map[string]interface{}:
		cb, _ := json.Marshal(data.(map[string]interface{}))
		c = string(cb)
		break
	default:
		c = fmt.Sprint(data)
		break
	}
	err := This.httpPost("update",key,c)
	if err != nil{
		return false,err
	}
	return true,nil
}

func (This *Conn) Del(key string) (bool,error) {
	err := This.httpPost("delete",key,"")
	if err != nil{
		return false,err
	}
	return true,nil
}

func (This *Conn) SendToList(key string, data interface{}) (bool,error) {
	return false,nil
}