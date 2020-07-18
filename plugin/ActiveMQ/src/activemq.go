package src

import (
	"encoding/json"
	"log"
	"strconv"
	"strings"
	"time"
	"github.com/gmallard/stompngo"
	"net"
	pluginDriver "github.com/brokercap/Bifrost/plugin/driver"
	"fmt"
)

type Conn struct {
	Uri    string
	status string
	conn   *stompngo.Connection
	err    error
	header stompngo.Headers
	p	   *PluginParam
}

type PluginParam struct {
	QueueName 			string
	Persistent 			bool
	Expir 				int
}


func newConn(uri string) *Conn{
	f := &Conn{
		Uri:uri,
		status:"close",
	}
	f.Connect()
	return f
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
	if param.QueueName == ""{
		return nil,fmt.Errorf("QueueName is empty")
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
	This.Close()
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
	if This.conn != nil {
		func(){
			defer func() {
				if err:=recover();err!=nil{
					return
				}
			}()
			This.conn.Disconnect(This.header)
		}()
	}
	return true
}

func (This *Conn) Insert(data *pluginDriver.PluginDataType) (*pluginDriver.PluginBinlog,error) {
	return This.sendToList(data)
}

func (This *Conn) Update(data *pluginDriver.PluginDataType) (*pluginDriver.PluginBinlog,error) {
	return This.sendToList(data)
}

func (This *Conn) Del(data *pluginDriver.PluginDataType) (*pluginDriver.PluginBinlog,error) {
	return This.sendToList(data)
}

func (This *Conn) Query(data *pluginDriver.PluginDataType) (*pluginDriver.PluginBinlog,error) {
	return This.sendToList(data)
}

func (This *Conn) sendToList(data *pluginDriver.PluginDataType) (*pluginDriver.PluginBinlog,error) {
	if This.status != "running"{
		This.ReConnect()
		if This.status != "running"{
			return nil,This.err
		}
	}
	c,err := json.Marshal(data)
	if err != nil{
		This.err = err
		return nil,err
	}
	QueueName := fmt.Sprint(pluginDriver.TransfeResult(This.p.QueueName, data, len(data.Rows)-1))
	var h stompngo.Headers
	h = h.Add(stompngo.HK_DESTINATION,QueueName)
	if This.p.Persistent == true{
		h = h.Add("persistent","true")
	}
	if This.p.Expir > 0{
		h = h.Add("expires",strconv.FormatInt((time.Now().Unix()*1000+int64(This.p.Expir)),10))
	}
	err2 := This.conn.SendBytes(h,c)
	if err2 !=nil{
		This.err = err2
		This.status="close"
		return nil,err
	}
	return &pluginDriver.PluginBinlog{data.BinlogFileNum,data.BinlogPosition},nil
}

func (This *Conn) Commit() (*pluginDriver.PluginBinlog,error){
	return nil,nil
}
