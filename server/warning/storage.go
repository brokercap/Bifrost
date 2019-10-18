package warning

import (
	"github.com/brokercap/Bifrost/server/storage"
	"encoding/json"
	"strings"
	"strconv"
	"log"
)

const WARNING_KEY_PREFIX  = "bifrost_warning_config_"

type WaringConfig struct {
	Type string
	Param map[string]interface{}
}

var allWaringConfigCacheMap map[string]WaringConfig

var firstStartUp bool = true
var lastConfigID int = 0


func init()  {
	allWaringConfigCacheMap = make(map[string]WaringConfig,0)
}

func getLastConfigID() int{
	l.Lock()
	lastConfigID++
	ID := lastConfigID
	l.Unlock()
	return ID
}

func getNewWaringKey() string{
	return WARNING_KEY_PREFIX+ strconv.Itoa(getLastConfigID())
}

func getWaringKey(ID int) string{
	return WARNING_KEY_PREFIX+ strconv.Itoa(ID)
}

func InitWarningConfigCache(){
	if firstStartUp == false{
		return
	}
	firstStartUp = false
	data := storage.GetListByPrefix([]byte(WARNING_KEY_PREFIX))
	for _,v := range data{
		key := v.Key
		t := strings.Split(key,"_")
		idString := t[len(t)-1]
		intA, err := strconv.Atoi(idString)
		if err != nil{
			continue
		}
		if intA > lastConfigID{
			lastConfigID = intA
		}

		var tmpWarningConfig WaringConfig
		err2 := json.Unmarshal([]byte(v.Value),&tmpWarningConfig)
		if err2 != nil{
			continue
		}
		addWarningConfigCache(key,tmpWarningConfig)
	}
}

func GetWarningConfigList() map[string]WaringConfig{
	InitWarningConfigCache()
	return allWaringConfigCacheMap
}

func addWarningConfigCache(key string,config WaringConfig){
	l.Lock()
	allWaringConfigCacheMap[key] = config
	l.Unlock()
}

func delWarningConfigCache(key string){
	l.Lock()
	delete(allWaringConfigCacheMap,key)
	l.Unlock()
}

func AddNewWarningConfig(p WaringConfig) (string,error) {
	InitWarningConfigCache()
	b,_ := json.Marshal(p)
	key := getNewWaringKey()
	addWarningConfigCache(key,p)
	return key,storage.PutKeyVal([]byte(key),b)
}

func DelWarningConfig(ID int) error {
	key := getWaringKey(ID)
	delWarningConfigCache(key)
	return storage.DelKeyVal([]byte(key))
}

func RecoveryWarning(content *json.RawMessage)  {
	if content == nil{
		return
	}
	var data map[string]WaringConfig
	errors := json.Unmarshal(*content,&data)
	if errors != nil{
		log.Println( "recorery warning content errors;",errors," content:",content)
		return
	}
	for key,v := range data{
		b,_ := json.Marshal(v)
		storage.PutKeyVal([]byte(key),b)
	}
	firstStartUp = false
}