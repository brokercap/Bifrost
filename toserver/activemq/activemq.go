package activemq

import (
	"fmt"
	"encoding/json"
	dataDriver "database/sql/driver"
	"log"
	"strconv"
	"strings"
	"time"
	"github.com/gmallard/stompngo"
	"net"
)

type Conn struct {
	Uri    string
	status string
	conn   *stompngo.Connection
	err    error
	expir  string
	mustBeSuccess bool
	header stompngo.Headers
}

func newConn(uri string) *Conn{
	f := &Conn{
		Uri:uri,
		expir:"",
		status:"close",
		mustBeSuccess:false,
	}
	f.Connect()
	return f
}

func (This *Conn) GetConnStatus() string {
	return This.status
}

func (This *Conn) SetConnStatus(status string) {
	This.status = status
}


func (This *Conn) Connect() bool {
	user,pwd,uri := getUriParam(This.Uri)
	n, e := net.Dial("tcp", uri)
	if e != nil{
		log.Println("net conn err:",e)
		This.err = e
		This.status = "close"
		return false
	}
	if user == ""{
		This.header = stompngo.Headers{}
	}else{
		This.header = stompngo.Headers{stompngo.HK_LOGIN,user,stompngo.HK_PASSCODE,pwd}
	}

	This.conn, e = stompngo.Connect(n, This.header)
	if e != nil{
		This.err = e
		This.status = "close"
		return false
	}
	This.status = "running"
	return true
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

func (This *Conn) ReConnect() bool {
	func(){
		defer func(){
			if err := recover();err != nil{
				return
			}
		}()
		This.conn.Disconnect(This.header)
	}()
	r := This.Connect()
	if r == true{
		return  true
	}else{
		return  false
	}
}

func (This *Conn) HeartCheck() {
	return
}

func (This *Conn) Close() bool {
	This.conn.Disconnect(This.header)
	return true
}

func (This *Conn) Insert(key string, data interface{}) (bool,error) {
	return false,fmt.Errorf("not support insert")
}

func (This *Conn) Update(key string, data interface{}) (bool,error) {
	return false,fmt.Errorf("not support update")
}

func (This *Conn) Del(key string) (bool,error) {
	return false,fmt.Errorf("not support delete")
}

func (This *Conn) SetExpir(TimeOut int) {
	if TimeOut > 0 {
		This.expir = strconv.FormatInt((time.Now().Unix()+int64(TimeOut))*1000,10)
	}else{
		This.expir = ""
	}
}

func (This *Conn) SetMustBeSuccess(b bool) {
	return
}


func (This *Conn) SendToList(key string, data interface{}) (bool,error) {
	if This.status != "running"{
		This.ReConnect()
		if This.status != "running"{
			return false,This.err
		}
	}

	s := strings.Split(key,"#")
	var DeliveryMode bool = false
	var DESTINATION string = ""
	switch len(s) {
	case 2:
		DESTINATION = s[0]
		if s[1] != "false"{
			DeliveryMode = true
		}
		break
	case 1:
		DESTINATION = s[0]
		break
	default:
		This.err = fmt.Errorf("key must queue_name[#persistent]")
		This.status = "error"
		return false,fmt.Errorf("key must be queue_name[#persistent]")
	}
	var c []byte
	switch data.(type){
	case string:
		c = []byte(data.(string))
	case map[string]dataDriver.Value:
		c,_=json.Marshal(data)
		break
	default:
		return false,fmt.Errorf("data must be a string or a map")
	}
	var h stompngo.Headers
	h = h.Add(stompngo.HK_DESTINATION,DESTINATION)
	if DeliveryMode == true{
		h = h.Add("persistent","true")
	}
	if This.expir != ""{
		h = h.Add("expires",This.expir)
	}
	err := This.conn.SendBytes(h,c)
	if err !=nil{
		This.err = err
		This.status="close"
		return false,err
	}
	return true,nil
}
