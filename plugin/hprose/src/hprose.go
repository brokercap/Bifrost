package src

import (
	"fmt"
	pluginDriver "github.com/brokercap/Bifrost/plugin/driver"
	"github.com/hprose/hprose-golang/rpc"
)

const VERSION = "v1.6.0"
const BIFROST_VERION = "v1.6.0"

func init() {
	pluginDriver.Register("hprose", NewConn, VERSION, BIFROST_VERION)
}

type Stub struct {
	Check  func() error
	Insert func(SchemaName string, TableName string, data map[string]interface{}) (e error)
	Update func(SchemaName string, TableName string, data []map[string]interface{}) (e error)
	Delete func(SchemaName string, TableName string, data map[string]interface{}) (e error)
	Query  func(SchemaName string, TableName string, sql string) (e error)
}

var stub *Stub

type Conn struct {
	pluginDriver.PluginDriverInterface
	Uri           *string
	status        string
	httpClient    *rpc.HTTPClient
	tcpClient     *rpc.TCPClient
	defaultClient rpc.Client
	clientType    string
	err           error
}

func NewConn() pluginDriver.Driver {
	f := &Conn{
		status: "close",
	}
	return f
}

func (This *Conn) SetOption(uri *string, param map[string]interface{}) {
	This.Uri = uri
	return
}

func (This *Conn) SetParam(p interface{}) (interface{}, error) {
	return nil, nil
}

func (This *Conn) Open() error {
	This.Connect()
	return nil
}

func (This *Conn) GetUriExample() string {
	return "http://127.0.0.1:61613 or tcp4://127.0.0.1:4321/"
}

func (This *Conn) CheckUri() error {
	This.Connect()
	if This.status != "running" {
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

func checkUriType(uri string) string {
	if uri[0:3] == "tcp" {
		return "tcp"
	}
	if uri[0:4] == "http" {
		return "http"
	}
	return ""
}

func (This *Conn) Connect() bool {
	This.clientType = checkUriType(*This.Uri)
	switch This.clientType {
	case "tcp":
		This.tcpClient = rpc.NewTCPClient(*This.Uri)
		This.tcpClient.UseService(&stub)
		break
	case "http":
		This.httpClient = rpc.NewHTTPClient(*This.Uri)
		This.httpClient.UseService(&stub)
		break
	default:
		This.defaultClient = rpc.NewClient(*This.Uri)
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

func (This *Conn) Close() bool {
	This.clientType = checkUriType(*This.Uri)
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
	return true
}

func (This *Conn) Insert(data *pluginDriver.PluginDataType, retry bool) (*pluginDriver.PluginDataType, *pluginDriver.PluginDataType, error) {
	err := stub.Insert(data.SchemaName, data.TableName, data.Rows[0])
	if err != nil {
		This.err = err
		return nil, data, err
	}
	return nil, nil, nil
}

func (This *Conn) Update(data *pluginDriver.PluginDataType, retry bool) (*pluginDriver.PluginDataType, *pluginDriver.PluginDataType, error) {
	err := stub.Update(data.SchemaName, data.TableName, data.Rows)
	if err != nil {
		This.err = err
		return nil, data, err
	}
	return nil, nil, nil
}

func (This *Conn) Del(data *pluginDriver.PluginDataType, retry bool) (*pluginDriver.PluginDataType, *pluginDriver.PluginDataType, error) {
	err := stub.Delete(data.SchemaName, data.TableName, data.Rows[0])
	if err != nil {
		This.err = err
		return nil, data, err
	}
	return nil, nil, nil
}

func (This *Conn) Query(data *pluginDriver.PluginDataType, retry bool) (*pluginDriver.PluginDataType, *pluginDriver.PluginDataType, error) {
	err := stub.Query(data.SchemaName, data.TableName, data.Query)
	if err != nil {
		This.err = err
		return nil, data, err
	}
	return nil, nil, nil
}

func (This *Conn) Commit(data *pluginDriver.PluginDataType, retry bool) (*pluginDriver.PluginDataType, *pluginDriver.PluginDataType, error) {
	err := stub.Query(data.SchemaName, data.TableName, data.Query)
	if err != nil {
		This.err = err
		return nil, data, err
	}
	return data, nil, nil
}
