package driver

import (
	"sync"
	"fmt"
	"regexp"
	"strings"
	"github.com/jc3wish/Bifrost/Bristol/mysql"
)

func init(){

}

type Driver interface {
	Open(uri string) ConnFun
	//GetTypeList() []string
	GetUriExample() string
	CheckUri(uri string) error
	GetTypeAndRule() TypeAndRule
	GetDoc() string
}

type ConnFun interface {
	GetConnStatus() string
	SetConnStatus(status string)
	Connect() bool
	ReConnect() bool
	HeartCheck()
	Close() bool
	Insert(key string, data interface{}) (bool,error)
	Update(key string, data interface{}) (bool,error)
	Del(key string) (bool,error)
	SendToList(key string, data interface{}) (bool,error)
	SetExpir(Expir int)
	SetMustBeSuccess(b bool)
	SetParam(p interface{})
}

type TypeRule struct {
	Key string
	KeyExample string
	Val string
}

type TypeAndRule struct {
	DataTypeList []string
	TypeList map[string]TypeRule
}

var (
	driversMu sync.RWMutex
	drivers   = make(map[string]Driver)
)

func Register(name string, driver Driver) {
	driversMu.Lock()
	defer driversMu.Unlock()
	if driver == nil {
		panic("sql: Register driver is nil")
	}
	if _, ok := drivers[name]; ok {
		panic("sql: Register called twice for driver " + name)
	}
	drivers[name] = driver
}

func Drivers() []map[string]string {
	driversMu.RLock()
	defer driversMu.RUnlock()
	var list []map[string]string
	for name,v := range drivers {
		m := make(map[string]string)
		m["name"] = name
		m["uri"] = v.GetUriExample()
		list = append(list, m)
	}
	return list
}

func Open(name string,uri string) ConnFun{
	driversMu.RLock()
	defer driversMu.RUnlock()
	if _,ok := drivers[name];!ok{
		return nil
	}
	return drivers[name].Open(uri)
}

func GetTypeAndRule(name string) TypeAndRule{
	driversMu.RLock()
	defer driversMu.RUnlock()
	if _,ok := drivers[name];!ok{
		return TypeAndRule{}
	}
	return drivers[name].GetTypeAndRule()
}

func CheckUri(name string,uri string) error{
	driversMu.RLock()
	defer driversMu.RUnlock()
	if _,ok := drivers[name];!ok{
		return fmt.Errorf("no "+name)
	}
	return drivers[name].CheckUri(uri)
}

func GetDocs() map[string]string{
	driversMu.RLock()
	defer driversMu.RUnlock()
	docs := make(map[string]string,0)
	for name,v := range drivers {
		docs[name] = v.GetDoc()
	}
	return docs
}


func evenTypeName(e mysql.EventType) string {
	switch e {
	case mysql.WRITE_ROWS_EVENTv0, mysql.WRITE_ROWS_EVENTv1, mysql.WRITE_ROWS_EVENTv2:
		return "insert"
	case mysql.UPDATE_ROWS_EVENTv0, mysql.UPDATE_ROWS_EVENTv1, mysql.UPDATE_ROWS_EVENTv2:
		return "update"
	case mysql.DELETE_ROWS_EVENTv0, mysql.DELETE_ROWS_EVENTv1, mysql.DELETE_ROWS_EVENTv2:
		return "delete"
	}
	return fmt.Sprintf("%d", e)
}

const RegularxEpression  = `\{\$([a-zA-Z0-9\-\_]+)\}`

func transfeResult(val string, data *mysql.EventReslut,rowIndex int) string {
	r, _ := regexp.Compile(RegularxEpression)
	p := r.FindAllStringSubmatch(val, -1)
	for _, v := range p {
		switch v[1] {
		case "TableName":
			val = strings.Replace(val, "{$TableName}", data.TableName, -1)
			break
		case "SchemaName":
			val = strings.Replace(val, "{$SchemaName}", data.SchemaName, -1)
			break
		case "EventType":
			val = strings.Replace(val, "{$EventType}", evenTypeName(data.Header.EventType), -1)
			break
		default:
			val = strings.Replace(val, v[0], fmt.Sprint(data.Rows[rowIndex][v[1]]), -1)
			break
		}
	}
	return val
}
