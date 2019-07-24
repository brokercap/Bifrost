package pluginTestData

import (
	pluginDriver "github.com/brokercap/Bifrost/plugin/driver"
	"time"
	"encoding/json"
	"math/rand"
	"strings"
	"strconv"
	"fmt"
	"bytes"
	"encoding/gob"
)

var columnJsonString = `[{"ColumnName":"id","ColumnKey":"PRI","ColumnDefault":"NULL","DataType":"int","Extra":"auto_increment","ColumnType":"int(11) unsigned","CharacterSetName":"NULL","CollationName":"NULL","NumbericScale":0,"IsBool":false,"Unsigned":true,"IsPrimary":true,"AutoIncrement":true,"EnumValues":[],"SetValues":[],"CharacterMaximumLength":0,"NumbericPrecision":10,"Value":null},{"ColumnName":"testtinyint","ColumnKey":"","ColumnDefault":"-1","DataType":"tinyint","Extra":"","ColumnType":"tinyint(4)","CharacterSetName":"NULL","CollationName":"NULL","NumbericScale":0,"IsBool":false,"Unsigned":false,"IsPrimary":false,"AutoIncrement":false,"EnumValues":[],"SetValues":[],"CharacterMaximumLength":0,"NumbericPrecision":3,"Value":null},{"ColumnName":"testsmallint","ColumnKey":"","ColumnDefault":"-2","DataType":"smallint","Extra":"","ColumnType":"smallint(6)","CharacterSetName":"NULL","CollationName":"NULL","NumbericScale":0,"IsBool":false,"Unsigned":false,"IsPrimary":false,"AutoIncrement":false,"EnumValues":[],"SetValues":[],"CharacterMaximumLength":0,"NumbericPrecision":5,"Value":null},{"ColumnName":"testmediumint","ColumnKey":"","ColumnDefault":"-3","DataType":"mediumint","Extra":"","ColumnType":"mediumint(8)","CharacterSetName":"NULL","CollationName":"NULL","NumbericScale":0,"IsBool":false,"Unsigned":false,"IsPrimary":false,"AutoIncrement":false,"EnumValues":[],"SetValues":[],"CharacterMaximumLength":0,"NumbericPrecision":7,"Value":null},{"ColumnName":"testint","ColumnKey":"","ColumnDefault":"-4","DataType":"int","Extra":"","ColumnType":"int(11)","CharacterSetName":"NULL","CollationName":"NULL","NumbericScale":0,"IsBool":false,"Unsigned":false,"IsPrimary":false,"AutoIncrement":false,"EnumValues":[],"SetValues":[],"CharacterMaximumLength":0,"NumbericPrecision":10,"Value":null},{"ColumnName":"testbigint","ColumnKey":"","ColumnDefault":"-5","DataType":"bigint","Extra":"","ColumnType":"bigint(20)","CharacterSetName":"NULL","CollationName":"NULL","NumbericScale":0,"IsBool":false,"Unsigned":false,"IsPrimary":false,"AutoIncrement":false,"EnumValues":[],"SetValues":[],"CharacterMaximumLength":0,"NumbericPrecision":19,"Value":null},{"ColumnName":"testvarchar","ColumnKey":"","ColumnDefault":"NULL","DataType":"varchar","Extra":"","ColumnType":"varchar(10)","CharacterSetName":"utf8","CollationName":"utf8_general_ci","NumbericScale":0,"IsBool":false,"Unsigned":false,"IsPrimary":false,"AutoIncrement":false,"EnumValues":[],"SetValues":[],"CharacterMaximumLength":10,"NumbericPrecision":0,"Value":null},{"ColumnName":"testchar","ColumnKey":"","ColumnDefault":"NULL","DataType":"char","Extra":"","ColumnType":"char(2)","CharacterSetName":"utf8","CollationName":"utf8_general_ci","NumbericScale":0,"IsBool":false,"Unsigned":false,"IsPrimary":false,"AutoIncrement":false,"EnumValues":[],"SetValues":[],"CharacterMaximumLength":2,"NumbericPrecision":0,"Value":null},{"ColumnName":"testenum","ColumnKey":"","ColumnDefault":"en1","DataType":"enum","Extra":"","ColumnType":"enum('en1','en2','en3')","CharacterSetName":"utf8","CollationName":"utf8_general_ci","NumbericScale":0,"IsBool":false,"Unsigned":false,"IsPrimary":false,"AutoIncrement":false,"EnumValues":["en1","en2","en3"],"SetValues":[],"CharacterMaximumLength":3,"NumbericPrecision":0,"Value":null},{"ColumnName":"testset","ColumnKey":"","ColumnDefault":"set1","DataType":"set","Extra":"","ColumnType":"set('set1','set2','set3')","CharacterSetName":"utf8","CollationName":"utf8_general_ci","NumbericScale":0,"IsBool":false,"Unsigned":false,"IsPrimary":false,"AutoIncrement":false,"EnumValues":[],"SetValues":["set1","set2","set3"],"CharacterMaximumLength":14,"NumbericPrecision":0,"Value":null},{"ColumnName":"testtime","ColumnKey":"","ColumnDefault":"00:00:00","DataType":"time","Extra":"","ColumnType":"time","CharacterSetName":"NULL","CollationName":"NULL","NumbericScale":0,"IsBool":false,"Unsigned":false,"IsPrimary":false,"AutoIncrement":false,"EnumValues":[],"SetValues":[],"CharacterMaximumLength":0,"NumbericPrecision":0,"Value":null},{"ColumnName":"testdate","ColumnKey":"","ColumnDefault":"0000-00-00","DataType":"date","Extra":"","ColumnType":"date","CharacterSetName":"NULL","CollationName":"NULL","NumbericScale":0,"IsBool":false,"Unsigned":false,"IsPrimary":false,"AutoIncrement":false,"EnumValues":[],"SetValues":[],"CharacterMaximumLength":0,"NumbericPrecision":0,"Value":null},{"ColumnName":"testyear","ColumnKey":"","ColumnDefault":"1989","DataType":"year","Extra":"","ColumnType":"year(4)","CharacterSetName":"NULL","CollationName":"NULL","NumbericScale":0,"IsBool":false,"Unsigned":false,"IsPrimary":false,"AutoIncrement":false,"EnumValues":[],"SetValues":[],"CharacterMaximumLength":0,"NumbericPrecision":0,"Value":null},{"ColumnName":"testtimestamp","ColumnKey":"","ColumnDefault":"CURRENT_TIMESTAMP","DataType":"timestamp","Extra":"","ColumnType":"timestamp","CharacterSetName":"NULL","CollationName":"NULL","NumbericScale":0,"IsBool":false,"Unsigned":false,"IsPrimary":false,"AutoIncrement":false,"EnumValues":[],"SetValues":[],"CharacterMaximumLength":0,"NumbericPrecision":0,"Value":null},{"ColumnName":"testdatetime","ColumnKey":"","ColumnDefault":"0000-00-00 00:00:00","DataType":"datetime","Extra":"","ColumnType":"datetime","CharacterSetName":"NULL","CollationName":"NULL","NumbericScale":0,"IsBool":false,"Unsigned":false,"IsPrimary":false,"AutoIncrement":false,"EnumValues":[],"SetValues":[],"CharacterMaximumLength":0,"NumbericPrecision":0,"Value":null},{"ColumnName":"testfloat","ColumnKey":"","ColumnDefault":"0.00","DataType":"float","Extra":"","ColumnType":"float(9,2)","CharacterSetName":"NULL","CollationName":"NULL","NumbericScale":2,"IsBool":false,"Unsigned":false,"IsPrimary":false,"AutoIncrement":false,"EnumValues":[],"SetValues":[],"CharacterMaximumLength":0,"NumbericPrecision":9,"Value":null},{"ColumnName":"testdouble","ColumnKey":"","ColumnDefault":"0.00","DataType":"double","Extra":"","ColumnType":"double(9,2)","CharacterSetName":"NULL","CollationName":"NULL","NumbericScale":2,"IsBool":false,"Unsigned":false,"IsPrimary":false,"AutoIncrement":false,"EnumValues":[],"SetValues":[],"CharacterMaximumLength":0,"NumbericPrecision":9,"Value":null},{"ColumnName":"testdecimal","ColumnKey":"","ColumnDefault":"0.00","DataType":"decimal","Extra":"","ColumnType":"decimal(9,2)","CharacterSetName":"NULL","CollationName":"NULL","NumbericScale":2,"IsBool":false,"Unsigned":false,"IsPrimary":false,"AutoIncrement":false,"EnumValues":[],"SetValues":[],"CharacterMaximumLength":0,"NumbericPrecision":9,"Value":null},{"ColumnName":"testtext","ColumnKey":"","ColumnDefault":"NULL","DataType":"text","Extra":"","ColumnType":"text","CharacterSetName":"utf8","CollationName":"utf8_general_ci","NumbericScale":0,"IsBool":false,"Unsigned":false,"IsPrimary":false,"AutoIncrement":false,"EnumValues":[],"SetValues":[],"CharacterMaximumLength":65535,"NumbericPrecision":0,"Value":null},{"ColumnName":"testblob","ColumnKey":"","ColumnDefault":"NULL","DataType":"blob","Extra":"","ColumnType":"blob","CharacterSetName":"NULL","CollationName":"NULL","NumbericScale":0,"IsBool":false,"Unsigned":false,"IsPrimary":false,"AutoIncrement":false,"EnumValues":[],"SetValues":[],"CharacterMaximumLength":65535,"NumbericPrecision":0,"Value":null},{"ColumnName":"testbit","ColumnKey":"","ColumnDefault":"","DataType":"bit","Extra":"","ColumnType":"bit(8)","CharacterSetName":"NULL","CollationName":"NULL","NumbericScale":0,"IsBool":false,"Unsigned":false,"IsPrimary":false,"AutoIncrement":false,"EnumValues":[],"SetValues":[],"CharacterMaximumLength":0,"NumbericPrecision":8,"Value":null},{"ColumnName":"testbool","ColumnKey":"","ColumnDefault":"0","DataType":"tinyint","Extra":"","ColumnType":"tinyint(1)","CharacterSetName":"NULL","CollationName":"NULL","NumbericScale":0,"IsBool":true,"Unsigned":false,"IsPrimary":false,"AutoIncrement":false,"EnumValues":[],"SetValues":[],"CharacterMaximumLength":0,"NumbericPrecision":3,"Value":null},{"ColumnName":"testmediumblob","ColumnKey":"","ColumnDefault":"NULL","DataType":"mediumblob","Extra":"","ColumnType":"mediumblob","CharacterSetName":"NULL","CollationName":"NULL","NumbericScale":0,"IsBool":false,"Unsigned":false,"IsPrimary":false,"AutoIncrement":false,"EnumValues":[],"SetValues":[],"CharacterMaximumLength":16777215,"NumbericPrecision":0,"Value":null},{"ColumnName":"testlongblob","ColumnKey":"","ColumnDefault":"NULL","DataType":"longblob","Extra":"","ColumnType":"longblob","CharacterSetName":"NULL","CollationName":"NULL","NumbericScale":0,"IsBool":false,"Unsigned":false,"IsPrimary":false,"AutoIncrement":false,"EnumValues":[],"SetValues":[],"CharacterMaximumLength":4294967295,"NumbericPrecision":0,"Value":null},{"ColumnName":"testtinyblob","ColumnKey":"","ColumnDefault":"NULL","DataType":"tinyblob","Extra":"","ColumnType":"tinyblob","CharacterSetName":"NULL","CollationName":"NULL","NumbericScale":0,"IsBool":false,"Unsigned":false,"IsPrimary":false,"AutoIncrement":false,"EnumValues":[],"SetValues":[],"CharacterMaximumLength":255,"NumbericPrecision":0,"Value":null},{"ColumnName":"test_unsinged_tinyint","ColumnKey":"","ColumnDefault":"1","DataType":"tinyint","Extra":"","ColumnType":"tinyint(4) unsigned","CharacterSetName":"NULL","CollationName":"NULL","NumbericScale":0,"IsBool":false,"Unsigned":true,"IsPrimary":false,"AutoIncrement":false,"EnumValues":[],"SetValues":[],"CharacterMaximumLength":0,"NumbericPrecision":3,"Value":null},{"ColumnName":"test_unsinged_smallint","ColumnKey":"","ColumnDefault":"2","DataType":"smallint","Extra":"","ColumnType":"smallint(6) unsigned","CharacterSetName":"NULL","CollationName":"NULL","NumbericScale":0,"IsBool":false,"Unsigned":true,"IsPrimary":false,"AutoIncrement":false,"EnumValues":[],"SetValues":[],"CharacterMaximumLength":0,"NumbericPrecision":5,"Value":null},{"ColumnName":"test_unsinged_mediumint","ColumnKey":"","ColumnDefault":"3","DataType":"mediumint","Extra":"","ColumnType":"mediumint(8) unsigned","CharacterSetName":"NULL","CollationName":"NULL","NumbericScale":0,"IsBool":false,"Unsigned":true,"IsPrimary":false,"AutoIncrement":false,"EnumValues":[],"SetValues":[],"CharacterMaximumLength":0,"NumbericPrecision":7,"Value":null},{"ColumnName":"test_unsinged_int","ColumnKey":"","ColumnDefault":"4","DataType":"int","Extra":"","ColumnType":"int(11) unsigned","CharacterSetName":"NULL","CollationName":"NULL","NumbericScale":0,"IsBool":false,"Unsigned":true,"IsPrimary":false,"AutoIncrement":false,"EnumValues":[],"SetValues":[],"CharacterMaximumLength":0,"NumbericPrecision":10,"Value":null},{"ColumnName":"test_unsinged_bigint","ColumnKey":"","ColumnDefault":"5","DataType":"bigint","Extra":"","ColumnType":"bigint(20) unsigned","CharacterSetName":"NULL","CollationName":"NULL","NumbericScale":0,"IsBool":false,"Unsigned":true,"IsPrimary":false,"AutoIncrement":false,"EnumValues":[],"SetValues":[],"CharacterMaximumLength":0,"NumbericPrecision":20,"Value":null}]`

type Column struct {
	ColumnName string
	ColumnKey string
	ColumnDefault string
	DataType string
	Extra string
	ColumnType string
	CharacterSetName string
	CollationName string
	NumbericScale int
	IsBool bool
	Unsigned bool
	IsPrimary bool
	AutoIncrement bool
	EnumValues []string
	SetValues []string
	CharacterMaximumLength int
	NumbericPrecision int
	Value interface{}
}

type EventType int8
const (
	RANDALL	EventType = -1
	INSERT EventType = 0
	UPDATE EventType = 1
	DELETE EventType = 2
	SQLTYPE EventType = 3
	COMMIT EventType = 4
	OTHERTYPE EventType = 5
)

func GetRandomString(l int,cn int) string {
	str := "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ^&*'\";\\/%$#@90-_|<>?{}[]+.!~`,=0"
	str2Arr := []string{"测","试","数","据"}
	bytes := []byte(str)
	result1 := []byte{}
	result2 := ""
	for i := 0; i < l; i++ {
		rand.Seed(time.Now().UnixNano()+int64(i))
		result1 = append(result1, bytes[rand.Intn(len(bytes))])
	}
	for i:=0;i < cn;i++{
		rand.Seed(time.Now().UnixNano()+int64(i))
		result2 += str2Arr[rand.Intn(len(str2Arr))]
	}
	rand.Seed(time.Now().UnixNano())
	return string(result1)+result2
}


type Event struct {
	Schema string
	Talbe  string
	AutoIncrementNum uint64
	ColumnList []*Column
	lastEventData map[string]interface{}
	position uint32
}

func NewEvent() *Event {
	var data []*Column
	json.Unmarshal([]byte(columnJsonString),&data)
	return &Event{
		Schema:"bifrost_test",
		Talbe:"binlog_field_test",
		AutoIncrementNum:1,
		ColumnList:data,
		lastEventData:make(map[string]interface{},0),
		position:0,
	}
}

func (This *Event) SetSchema(name string) *Event{
	This.Schema = name
	return This
}

func (This *Event) SetTable(name string) *Event{
	This.Talbe = name
	return This
}

func (This *Event) getSchemaTableFieldAndVal(columnList []*Column,eventType EventType ) ([]interface{},map[string]interface{}){
	data := make([]interface{},0)
	columnData := make(map[string]interface{},0)
	for _,columnType := range columnList{
		rand.Seed(time.Now().UnixNano())
		var randResult int
		if rand.Intn(2) >= 1{
			randResult = 1
		}else{
			randResult = 0
		}
		if columnType.AutoIncrement {
			if eventType == INSERT{
				This.AutoIncrementNum++
			}
			a:=This.AutoIncrementNum
			switch columnType.DataType {
			case "tinyint":
				if columnType.Unsigned == true{
					columnData[columnType.ColumnName] = uint8(a)
				}else{
					columnData[columnType.ColumnName] = int8(a)
				}
				break
			case "smallint":
				if columnType.Unsigned == true{
					columnData[columnType.ColumnName] = uint16(a)
				}else{
					columnData[columnType.ColumnName] = int16(a)
				}
				break
			case "mediumint","int":
				if columnType.Unsigned == true{
					columnData[columnType.ColumnName] = uint32(a)
				}else{
					columnData[columnType.ColumnName] = int32(a)
				}
				break
			case "bigint":
				if columnType.Unsigned == true{
					columnData[columnType.ColumnName] = uint64(a)
				}else{
					columnData[columnType.ColumnName] = int64(a)
				}
				break
			}
			data = append(data,columnData[columnType.ColumnName])
			continue
		}
		switch columnType.DataType {
		case "int", "tinyint", "smallint", "mediumint", "bigint":
			if columnType.IsBool{
				if randResult == 1{
					data = append(data,"1")
					columnType.Value = true
				}else{
					data = append(data,"0")
					columnType.Value = false
				}
			}else{
				var Value interface{}
				switch columnType.DataType {
				case "tinyint":
					if columnType.Unsigned == true{
						Value = uint8(255)
					}else{
						if randResult == 1{
							Value = int8(127)
						}else{
							Value = int8(-128)
						}
					}
					break
				case "smallint":
					if columnType.Unsigned == true{
						Value = uint16(65535)
					}else{
						if randResult == 1{
							Value = int16(32767)
						}else{
							Value = int16(-32768)
						}
					}
					break
				case "mediumint":
					if columnType.Unsigned == true{
						Value = uint32(16777215)
					}else{
						if randResult == 1{
							Value = int32(8388607)
						}else{
							Value = int32(-8388608)
						}
					}
					break
				case "int":
					if columnType.Unsigned == true{
						Value = uint32(4294967295)
					}else{
						if randResult == 1{
							Value = int32(2147483647)
						}else{
							Value = int32(-2147483648)
						}
					}
					break
				case "bigint":
					if columnType.Unsigned == true{
						Value = uint64(18446744073709551615)
					}else{
						if randResult == 1{
							Value = int64(9223372036854775807)
						}else{
							Value = int64(-9223372036854775808)
						}
					}
					break
				}
				columnType.Value = Value
				data = append(data,Value)
			}
			break
		case "char","varchar":
			var enSize,cnSize int = 0,0
			if strings.Contains(columnType.CharacterSetName,"utf"){
				if columnType.CharacterMaximumLength/4 > 0{
					cnSize = rand.Intn(columnType.CharacterMaximumLength/4)
					enSize = columnType.CharacterMaximumLength - cnSize*4
				}else{
					enSize = rand.Intn(columnType.CharacterMaximumLength-1)
				}
			}else{
				enSize = rand.Intn(columnType.CharacterMaximumLength-1)
			}
			Value := GetRandomString(enSize,cnSize)
			columnType.Value = Value
			data = append(data,Value)
			break
		case "tinytext","tinyblob","text","mediumtext","smalltext","blob","mediumblob","smallblob","longblob":
			var enSize,cnSize int = 0,0
			rand.Seed(time.Now().UnixNano())

			var n int

			n = rand.Intn(255/4)

			if n == 0{
				n = 1
			}
			if strings.Contains(columnType.CharacterSetName,"utf"){
				cnSize = rand.Intn(n)
			}
			enSize = n - cnSize
			Value := GetRandomString(enSize,cnSize)
			columnType.Value = Value
			data = append(data,Value)
			break
		case "year":
			Value := time.Now().Format("2006")
			columnType.Value = Value
			data = append(data,Value)
			break
		case "time":
			Value := time.Now().Format("15:04:05")
			columnType.Value = Value
			data = append(data,Value)
			break
		case "date":
			Value := time.Now().Format("2006-01-02")
			columnType.Value = Value
			data = append(data,Value)
			break
		case "datetime","timestamp":
			Value := time.Now().Format("2006-01-02 15:04:05")
			columnType.Value = Value
			data = append(data,Value)
			break
		case "bit":
			var Value int64 = 1
			if columnType.NumbericPrecision < 16{
				Value = int64(rand.Intn(127))
			}
			if columnType.NumbericPrecision >=16 && columnType.NumbericPrecision < 32{
				Value = int64(rand.Intn(32767))
			}
			if columnType.NumbericPrecision >= 32 && columnType.NumbericPrecision < 64{
				Value = int64(rand.Int31())
			}
			if columnType.NumbericPrecision == 64{
				Value = rand.Int63()
			}
			columnType.Value = Value
			data = append(data,Value)
			break
		case "float":
			Value := strconv.FormatFloat(float64(rand.Float32()),'f',2,32)
			Value2,_ := strconv.ParseFloat(Value, 32)
			f1 := float32(rand.Intn(999999))
			f2 := f1+float32(Value2)
			if randResult == 1{
				f2 = 0-f2
			}
			columnType.Value = f2
			data = append(data,f2)
			break
		case "double":
			Value := strconv.FormatFloat(float64(rand.Float64()),'f',2,64)
			Value2,_ := strconv.ParseFloat(Value, 64)
			f1 := float64(rand.Intn(999999))
			f2 := f1+float64(Value2)
			if randResult == 1{
				f2 = 0-f2
			}
			columnType.Value = f2
			data = append(data,f2)
			break
		case "decimal":
			Value := strconv.FormatFloat(float64(rand.Float64()),'f',2,64)
			Value2,_ := strconv.ParseFloat(Value, 64)
			f1 := float64(rand.Intn(999999))
			f2 := f1+float64(Value2)
			if randResult == 1{
				f2 = 0-f2
			}
			columnType.Value = fmt.Sprint(f2)
			data = append(data,fmt.Sprint(f2))
			break
		case "set":
			d := strings.Replace(columnType.ColumnType, "set(", "", -1)
			d = strings.Replace(d, ")", "", -1)
			d = strings.Replace(d, "'", "", -1)
			set_values := strings.Split(d, ",")
			Value := make([]string,0)
			if len(set_values) > 1{
				Value = append(Value,set_values[0])
				Value = append(Value,set_values[len(set_values)-1])
			}else{
				Value = append(Value,set_values[0])
			}
			columnType.Value = Value
			data = append(data,strings.Replace(strings.Trim(fmt.Sprint(Value), "[]"), " ", ",", -1))
			break
		case "enum":
			d := strings.Replace(columnType.ColumnType, "enum(", "", -1)
			d = strings.Replace(d, ")", "", -1)
			d = strings.Replace(d, "'", "", -1)
			enum_values := strings.Split(d, ",")
			Value := enum_values[rand.Intn(len(enum_values)-1)]
			columnType.Value = Value
			data = append(data,Value)
			break
		default:
			data = append(data,"0")
			break
		}

		columnData[columnType.ColumnName] = columnType.Value
	}

	This.lastEventData = columnData
	//log.Println("This.lastEventData:",This.lastEventData)
	return data,columnData
}

func (This *Event) GetTestInsertData() *pluginDriver.PluginDataType{
	Rows := make([]map[string]interface{},1)

	_, Rows[0] = This.getSchemaTableFieldAndVal(This.ColumnList,INSERT)

	This.position+=100
	return &pluginDriver.PluginDataType{
		Timestamp 		: uint32(time.Now().Unix()),
		EventType 		: "insert",
		Rows            : Rows,
		Query          	: "",
		SchemaName     	: This.Schema,
		TableName      	: This.Talbe,
		BinlogFileNum 	: 10,
		BinlogPosition 	: This.position,
	}
}

func(This *Event) deepCopy(dst, src interface{}) error {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(src); err != nil {
		return err
	}
	return gob.NewDecoder(bytes.NewBuffer(buf.Bytes())).Decode(dst)
}

func (This *Event) GetTestUpdateData() *pluginDriver.PluginDataType{
	Rows := make([]map[string]interface{},2)
	if len(This.lastEventData) == 0{
		This.getSchemaTableFieldAndVal(This.ColumnList,INSERT)
	}

	Rows[0] = This.lastEventData
	_, Rows[1] = This.getSchemaTableFieldAndVal(This.ColumnList,UPDATE)

	This.position+=100
	return &pluginDriver.PluginDataType{
		Timestamp 		: uint32(time.Now().Unix()),
		EventType 		: "update",
		Rows            : Rows,
		Query          	: "",
		SchemaName     	: This.Schema,
		TableName      	: This.Talbe,
		BinlogFileNum 	: 10,
		BinlogPosition 	: This.position,
	}
}

func (This *Event) GetTestDeleteData() *pluginDriver.PluginDataType{
	Rows := make([]map[string]interface{},1)

	if len(This.lastEventData) == 0{
		This.getSchemaTableFieldAndVal(This.ColumnList,INSERT)
	}

	Rows[0] = This.lastEventData

	This.position+=100
	return &pluginDriver.PluginDataType{
		Timestamp 		: uint32(time.Now().Unix()),
		EventType 		: "delete",
		Rows            : Rows,
		Query          	: "",
		SchemaName     	: This.Schema,
		TableName      	: This.Talbe,
		BinlogFileNum 	: 10,
		BinlogPosition 	: This.position,
	}
}

func (This *Event) GetTestQueryData() *pluginDriver.PluginDataType{
	var Rows []map[string]interface{}
	Rows = make([]map[string]interface{},0)

	This.position+=100
	return &pluginDriver.PluginDataType{
		Timestamp 		: uint32(time.Now().Unix()),
		EventType 		: "sql",
		Rows            : Rows,
		Query          	: "ALTER TABLE `"+This.Schema+"`.`"+This.Talbe+"` CHANGE COLUMN `testvarchar` `testvarchar` varchar(255) NOT NULL",
		SchemaName     	: This.Schema,
		TableName      	: This.Talbe,
		BinlogFileNum 	: 10,
		BinlogPosition 	: This.position,
	}
}

