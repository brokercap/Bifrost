package pluginTestData

import (
	"encoding/json"
	"fmt"
	"reflect"
)

type DataStruct struct {
	Id                      uint32 `json:"id,string"`
	Test_unsinged_bigint    uint64 `json:"test_unsinged_bigint,string"`
	Test_unsinged_int       uint32 `json:"test_unsinged_int,string"`
	Test_unsinged_mediumint uint32 `json:"test_unsinged_mediumint,string"`
	Test_unsinged_smallint  uint16 `json:"test_unsinged_smallint,string"`
	Test_unsinged_tinyint   uint8  `json:"test_unsinged_tinyint,string"`

	Testtinyint   int8  `json:"testtinyint,string"`
	Testsmallint  int16 `json:"testsmallint,string"`
	Testmediumint int32 `json:"testmediumint,string"`
	Testint       int32 `json:"testint,string"`
	Testbigint    int64 `json:"testbigint,string"`

	Testbit  int64 `json:"testbit,string"`
	Testbool bool  `json:"testbool"`

	Testvarchar string `json:"testvarchar"`
	Testchar    string `json:"testchar"`

	Testtime      string `json:"testtime"`
	Testdate      string `json:"testdate"`
	Testyear      string `json:"testyear"`
	Testtimestamp string `json:"testtimestamp"`
	Testdatetime  string `json:"testdatetime"`

	Testfloat   float32 `json:"testfloat"`
	Testdouble  float64 `json:"testdouble"`
	Testdecimal string  `json:"testdecimal"`

	Testtext       string `json:"testtext"`
	Testblob       string `json:"testblob"`
	Testmediumblob string `json:"testmediumblob"`
	Testlongblob   string `json:"testlongblob"`
	Testtinyblob   string `json:"testtinyblob"`

	Testenum string      `json:"testenum"`
	Testset  []string    `json:"testset"`
	Testjson interface{} `json:"Testjson"`
}

func (This *Event) CheckData(src map[string]interface{}, destJsonString string) (map[string][]string, error) {
	var err error
	var dest DataStruct
	err = json.Unmarshal([]byte(destJsonString), &dest)
	if err != nil {
		return nil, err
	}

	result := make(map[string][]string, 0)
	result["ok"] = make([]string, 0)
	result["error"] = make([]string, 0)

	var key string
	var srcV interface{}

	srcV = dest.Id
	key = "id"
	This.CheckData0(srcV, src[key], key, result)

	srcV = dest.Test_unsinged_bigint
	key = "test_unsinged_bigint"
	This.CheckData0(srcV, src[key], key, result)

	srcV = dest.Test_unsinged_int
	key = "test_unsinged_int"
	This.CheckData0(srcV, src[key], key, result)

	srcV = dest.Test_unsinged_mediumint
	key = "test_unsinged_mediumint"
	This.CheckData0(srcV, src[key], key, result)

	srcV = dest.Test_unsinged_smallint
	key = "test_unsinged_smallint"
	This.CheckData0(srcV, src[key], key, result)

	srcV = dest.Test_unsinged_tinyint
	key = "test_unsinged_tinyint"
	This.CheckData0(srcV, src[key], key, result)

	srcV = dest.Testtinyint
	key = "testtinyint"
	This.CheckData0(srcV, src[key], key, result)

	srcV = dest.Testsmallint
	key = "testsmallint"
	This.CheckData0(srcV, src[key], key, result)

	srcV = dest.Testmediumint
	key = "testmediumint"
	This.CheckData0(srcV, src[key], key, result)

	srcV = dest.Testint
	key = "testint"
	This.CheckData0(srcV, src[key], key, result)

	srcV = dest.Testbigint
	key = "testbigint"
	This.CheckData0(srcV, src[key], key, result)

	srcV = dest.Testbit
	key = "testbit"
	This.CheckData0(srcV, src[key], key, result)

	srcV = dest.Testbool
	key = "testbool"
	This.CheckData0(srcV, src[key], key, result)

	srcV = dest.Testdate
	key = "testdate"
	This.CheckData0(srcV, src[key], key, result)

	srcV = dest.Testyear
	key = "testyear"
	This.CheckData0(srcV, src[key], key, result)

	srcV = dest.Testvarchar
	key = "testvarchar"
	This.CheckData0(srcV, src[key], key, result)

	srcV = dest.Testchar
	key = "testchar"
	This.CheckData0(srcV, src[key], key, result)

	srcV = dest.Testtime
	key = "testtime"
	This.CheckData0(srcV, src[key], key, result)

	srcV = dest.Testdate
	key = "testdate"
	This.CheckData0(srcV, src[key], key, result)

	srcV = dest.Testyear
	key = "testyear"
	This.CheckData0(srcV, src[key], key, result)

	srcV = dest.Testtimestamp
	key = "testtimestamp"
	This.CheckData0(srcV, src[key], key, result)

	srcV = dest.Testdatetime
	key = "testdatetime"
	This.CheckData0(srcV, src[key], key, result)

	srcV = dest.Testfloat
	key = "testfloat"
	This.CheckData0(srcV, src[key], key, result)

	srcV = dest.Testdouble
	key = "testdouble"
	This.CheckData0(srcV, src[key], key, result)

	srcV = dest.Testdecimal
	key = "testdecimal"
	This.CheckData0(srcV, src[key], key, result)

	srcV = dest.Testtext
	key = "testtext"
	This.CheckData0(srcV, src[key], key, result)

	srcV = dest.Testblob
	key = "testblob"
	This.CheckData0(srcV, src[key], key, result)

	srcV = dest.Testmediumblob
	key = "testmediumblob"
	This.CheckData0(srcV, src[key], key, result)

	srcV = dest.Testlongblob
	key = "testlongblob"
	This.CheckData0(srcV, src[key], key, result)

	srcV = dest.Testtinyblob
	key = "testtinyblob"
	This.CheckData0(srcV, src[key], key, result)

	srcV = dest.Testenum
	key = "testenum"
	This.CheckData0(srcV, src[key], key, result)

	srcV = dest.Testset
	key = "testset"
	This.CheckData0(srcV, src[key], key, result)
	srcV = dest.Testjson
	key = "testjson"
	if reflect.ValueOf(srcV).Kind() == reflect.ValueOf(src[key]).Kind() {
		s := fmt.Sprint(key, " == ", srcV, " ( ", reflect.TypeOf(srcV), " ) ", " src val:", src[key], " ( ", reflect.TypeOf(src[key]), " ) ")
		result["ok"] = append(result["ok"], s)
	} else {
		s := fmt.Sprint(key, " ", srcV, " ( ", reflect.TypeOf(srcV), " ) ", " != ( ", src, reflect.TypeOf(src), " )")
		result["error"] = append(result["error"], s)
	}

	return result, nil
}

func (This *Event) CheckData2(src map[string]interface{}, destJsonString string) (map[string][]string, error) {
	type pluginType struct {
		Timestamp      uint32
		EventType      string
		Rows           []DataStruct
		Query          string
		SchemaName     string
		TableName      string
		BinlogFileNum  int
		BinlogPosition uint32
	}

	var data pluginType
	err := json.Unmarshal([]byte(destJsonString), &data)
	if err != nil {
		return nil, err
	}

	c, err := json.Marshal(data.Rows[len(data.Rows)-1])
	if err != nil {
		return nil, err
	}

	return This.CheckData(src, string(c))
}

func (This *Event) CheckData0(srcV interface{}, destV interface{}, key string, result map[string][]string) {
	if reflect.TypeOf(srcV) == reflect.TypeOf(destV) && fmt.Sprint(srcV) == fmt.Sprint(destV) {
		s := fmt.Sprint(key, " == ", srcV, " ( ", reflect.TypeOf(srcV), " ) ")
		result["ok"] = append(result["ok"], s)
	} else {
		s := fmt.Sprint(key, " ", srcV, " ( ", reflect.TypeOf(srcV), " ) ", " != ( ", destV, reflect.TypeOf(destV), " )")
		result["error"] = append(result["error"], s)
	}
}
