package hprose

import (
	"fmt"
	dataDriver "database/sql/driver"
	"github.com/hprose/hprose-golang/rpc"
)

type Stub struct {
	Check   func() error
	Insert  func(key string,timeout int, data map[string]dataDriver.Value) (e error)
	Update  func(key string,timeout int, data map[string]dataDriver.Value) (e error)
	Delete  func(key string) (e error)
	ToList  func(key string,timeout int, data map[string]dataDriver.Value) (e error)
}

var stub *Stub

type Conn struct {
	Uri    string
	status string
	httpClient *rpc.HTTPClient
	tcpClient *rpc.TCPClient
	defaultClient rpc.Client
	clientType string
	err    error
	expir  int
	mustBeSuccess bool
}

func newConn(uri string) *Conn{
	f := &Conn{
		Uri:uri,
		expir:0,
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

func checkUriType(uri string) string{
	if uri[0:3] == "tcp"{
		return "tcp"
	}
	if uri[0:4] == "http"{
		return "http"
	}
	return ""
}

func (This *Conn) Connect() bool {
	This.clientType = checkUriType(This.Uri)
	switch This.clientType {
	case "tcp":
		This.tcpClient = rpc.NewTCPClient(This.Uri)
		This.tcpClient.UseService(&stub)
		break
	case "http":
		This.httpClient = rpc.NewHTTPClient(This.Uri)
		This.httpClient.UseService(&stub)
		break
	default:
		This.defaultClient = rpc.NewClient(This.Uri)
		This.defaultClient.UseService(&stub)
		break
	}
	This.status = "running"
	return true
}


func (This *Conn) ReConnect() bool {
	This.Connect()
	return true
}

func (This *Conn) HeartCheck() {
	return
}

func (This *Conn) Close() bool {
	return true
}

func (This *Conn) SetExpir(TimeOut int) {
	This.expir = TimeOut
}

func (This *Conn) CheckUri() error {
	if This.status != "running"{
		return fmt.Errorf("not connect")
	}
	err := stub.Check()
	switch This.clientType {
	case "tcp":
		This.tcpClient.Close()
		break
	case "http":
		This.httpClient.Close()
		break
	default:
		This.defaultClient.Close()
		break
	}
	return err
}

func (This *Conn) Insert(key string, data interface{}) (bool,error) {
	err := stub.Insert(key,This.expir,data.(map[string]dataDriver.Value))
	if err != nil{
		This.err = err
		return false,err
	}
	return true,nil
}

func (This *Conn) Update(key string, data interface{}) (bool,error) {
	err := stub.Update(key,This.expir,data.(map[string]dataDriver.Value))
	if err != nil{
		This.err = err
		return false,err
	}
	return true,nil
}

func (This *Conn) Del(key string) (bool,error) {
	err := stub.Delete(key)
	if err != nil{
		This.err = err
		return false,err
	}
	return true,nil
}

func (This *Conn) SetMustBeSuccess(b bool) {
	return
}

func (This *Conn) SendToList(key string, data interface{}) (bool,error) {
	err := stub.ToList(key,This.expir,data.(map[string]dataDriver.Value))
	if err != nil{
		This.err = err
		return false,err
	}
	return true,nil
}
