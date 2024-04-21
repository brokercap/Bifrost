//go:build integration
// +build integration

package src

import (
	"context"
	"fmt"
	"log"
	"math"
	"math/rand"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	pluginDriver "github.com/brokercap/Bifrost/plugin/driver"
	"github.com/brokercap/Bifrost/sdk/pluginTestData"
	elastic "github.com/olivere/elastic/v7"
)

var myConn *Conn
var conn pluginDriver.Driver
var event *pluginTestData.Event
var SchemaName = "2bifrost_test"
var TableName = "binlog_field_test"
var EsIndexName = "{$SchemaName}--{$TableName}"
var Url = "http://192.168.220.130:9200"

func testBefore() {
	conn = NewConn()
	conn.SetOption(&Url, nil)
	conn.Open()
	event = pluginTestData.NewEvent()
	event.SetSchema(SchemaName)
	event.SetTable(TableName)
	event.SetNoUint64(true)
	myConn = conn.(*Conn)
}

func getParam(args ...bool) map[string]interface{} {
	param := map[string]interface{}{
		"PrimaryKey":           "id",        //            string
		"EsIndexName":          EsIndexName, //             string
		"BifrostMustBeSuccess": true,        //  bool  // bifrost server 保留,数据是否能丢
		"BatchSize":            3,           //             int
		"Mapping": `{
  "mappings": {
    "properties": {},
    "date_detection": true,
    "dynamic_date_formats": [
      "yyyy-MM-dd",
      "yyyy-MM-dd HH:mm:ss",
      "yyyy-MM-dd HH:mm:ss.S",
      "yyyy-MM-dd HH:mm:ss.SS",
      "yyyy-MM-dd HH:mm:ss.SSS",
      "yyyy-MM-dd HH:mm:ss.SSSS",
      "yyyy-MM-dd HH:mm:ss.SSSSS",
      "yyyy-MM-dd HH:mm:ss.SSSSSS"
    ]
  }
}`,
	}
	return param
}

func initSyncParam() {
	p, err := conn.SetParam(getParam())
	if err != nil {
		log.Println("set param fatal err")
		log.Fatal(err)
	}

	log.Println("Param:", p)
}

func TestConn_CheckUri(t *testing.T) {
	conn = NewConn()
	conn.SetOption(&Url, nil)
	err := conn.Open()
	if err != nil {
		t.Fatal(err)
	}
	t.Log("success")
}

func TestCommit(t *testing.T) {
	testBefore()
	initSyncParam()
	insertdata := event.GetTestInsertData()
	conn.Insert(insertdata, false)
	// log.Println("insertdata:", g.Export(insertdata))

	for ii := 0; ii < 10; ii++ {
		insertdata = event.GetTestInsertData()
		conn.Insert(insertdata, false)
	}

	conn.Del(event.GetTestDeleteData(), false)
	conn.Update(event.GetTestUpdateData(), false)

	conn.Insert(event.GetTestInsertData(), false)
	conn.Del(event.GetTestDeleteData(), false)
	conn.Insert(event.GetTestInsertData(), false)
	conn.Commit(event.GetTestCommitData(), false)
	_, _, err2 := conn.TimeOutCommit()
	if err2 != nil {
		t.Fatal(err2)
	}

	conn.Del(event.GetTestDeleteData(), false)
	conn.Update(event.GetTestUpdateData(), false)

	conn.Insert(event.GetTestInsertData(), false)
	conn.Del(event.GetTestDeleteData(), false)
	conn.Insert(event.GetTestInsertData(), false)
	conn.Insert(event.GetTestInsertData(), false)
	conn.Insert(event.GetTestInsertData(), false)
	conn.Commit(event.GetTestCommitData(), false)
	_, _, err2 = conn.TimeOutCommit()
	if err2 != nil {
		t.Fatal(err2)
	}
	t.Log("success")
}

func TestCommitPriKeyIsString(t *testing.T) {
	testBefore()
	initSyncParam()
	insertdata := event.GetTestInsertData()
	conn.Insert(insertdata, false)
	conn.Del(event.GetTestDeleteData(), false)
	conn.Update(event.GetTestUpdateData(), false)

	conn.Insert(event.GetTestInsertData(), false)
	conn.Del(event.GetTestDeleteData(), false)
	conn.Insert(event.GetTestInsertData(), false)
	_, _, err2 := conn.TimeOutCommit()
	if err2 != nil {
		t.Fatal(err2)
	}

	conn.Del(event.GetTestDeleteData(), false)
	conn.Update(event.GetTestUpdateData(), false)

	conn.Insert(event.GetTestInsertData(), false)
	conn.Del(event.GetTestDeleteData(), false)
	conn.Insert(event.GetTestInsertData(), false)
	conn.Insert(event.GetTestInsertData(), false)
	conn.Insert(event.GetTestInsertData(), false)
	_, _, err2 = conn.TimeOutCommit()
	if err2 != nil {
		t.Fatal(err2)
	}
	t.Log("success")
}

func TestReConnCommit(t *testing.T) {
	testBefore()
	initSyncParam()
	conn.Insert(event.GetTestInsertData(), false)
	_, _, err1 := conn.TimeOutCommit()
	if err1 != nil {
		t.Fatal("err1", err1)
		return
	} else {
		t.Log("insert 1 success")
	}

	conn.Del(event.GetTestDeleteData(), false)
	conn.Update(event.GetTestUpdateData(), false)
	time.Sleep(20 * time.Second)
	for {
		time.Sleep(3 * time.Second)
		_, _, err2 := conn.TimeOutCommit()
		if err2 != nil {
			t.Error("err2:", err2)
		} else {
			break
		}
	}
	t.Log("success")
}

func TestInsertNullAndChekcData(t *testing.T) {
	testBefore()

	initSyncParam()
	e := pluginTestData.NewEvent()
	e.SetIsNull(true)
	insertdata := e.GetTestInsertData()
	conn.Insert(insertdata, false)

	_, _, err2 := conn.TimeOutCommit()
	if err2 != nil {
		t.Fatal(err2)
	}
	dataList, _ := myConn.client.Get().Index(myConn.p.EsIndexName).Id(fmt.Sprint(insertdata.Rows[0]["id"])).Do(context.Background())
	// c := NewClickHouseDBConn(url)
	// dataList := c.GetTableDataList(insertdata.SchemaName, insertdata.TableName, "id="+fmt.Sprint(insertdata.Rows[0]["id"]))
	for k, v := range dataList.Fields {
		t.Log("k, v:", k, v)
	}
	t.Log("success")
}

func TestCommitAndCheckData(t *testing.T) {
	testBefore()
	initSyncParam()

	eventData := event.GetTestInsertData()
	conn.Insert(eventData, false)
	eventData = event.GetTestUpdateData()
	conn.Update(eventData, false)
	_, _, err2 := conn.TimeOutCommit()
	if err2 != nil {
		t.Fatal(err2)
	}

	m := eventData.Rows[len(eventData.Rows)-1]
	time.Sleep(1 * time.Second)
	// c := NewClickHouseDBConn(url)
	dataList, _ := myConn.client.Get().Index(myConn.p.EsIndexName).Id(fmt.Sprint(eventData.Rows[0]["id"])).Do(context.Background())

	resultData := make(map[string][]string, 0)
	resultData["ok"] = make([]string, 0)
	resultData["error"] = make([]string, 0)

	checkDataRight(m, dataList.Fields, resultData)

	// for _, v := range resultData["ok"] {
	// 	t.Log(v)
	// }

	for _, v := range resultData["error"] {
		t.Error(v)
	}

	if len(resultData["error"]) == 0 {
		t.Log("test over;", "data is all right")
	} else {
		t.Error("test over;", " some data is error")
	}

}

func TestRandDataAndCheck(t *testing.T) {

	var n int = 1000

	testBefore()

	initSyncParam()
	for i := 0; i < n; i++ {
		var eventData *pluginDriver.PluginDataType
		rand.Seed(time.Now().UnixNano() + int64(i))
		switch rand.Intn(3) {
		case 0:
			eventData = event.GetTestInsertData()
			conn.Insert(eventData, false)
			break
		case 1:
			eventData = event.GetTestUpdateData(true)
			if eventData != nil {
				conn.Update(eventData, false)
			}
			break
		case 2:
			eventData = event.GetTestDeleteData(true)
			if eventData != nil {
				conn.Del(eventData, false)
			}
			break
		case 3:
			eventData = event.GetTestQueryData()
			conn.Query(eventData, false)
			break
		}
	}
	conn.TimeOutCommit()

	dataMap := event.GetDataMap()

	mget := myConn.client.Mget()
	for id := range dataMap {
		q1 := &elastic.MultiGetItem{}
		q1.Index(myConn.p.EsIndexName).Id(fmt.Sprint(id))
		mget.Add(q1)
	}
	resultData := make(map[string][]string, 0)
	resultData["ok"] = make([]string, 0)
	resultData["error"] = make([]string, 0)

	time.Sleep(1 * time.Second)
	dataList, err := mget.Do(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	count := uint64(len(dataList.Docs))

	if count != uint64(len(dataMap)) {
		for k, v := range dataMap {
			_, _ = k, v
			// t.Log(k, " ", v)
		}
		t.Fatal("es Table Count:", count, " != srcDataCount:", len(dataMap))
	}
	destMap := make(map[string]map[string]interface{}, 0)

	for _, v := range dataList.Docs {
		destMap[fmt.Sprint(v.Fields["id"])] = v.Fields
	}
	for _, data := range dataMap {
		id := fmt.Sprint(data["id"])
		checkDataRight(data, destMap[id], resultData)
	}
	// for _, v := range resultData["ok"] {
	// 	t.Log(v)
	// }
	if len(resultData["error"]) > 0 {
		for _, v := range resultData["error"] {
			t.Error(v)
		}
	}

	t.Log("es Table Count:", count, " srcDataCount:", len(dataMap))

	t.Log("test over")
}

// 模拟正式环境刷数据
func TestSyncLikeProduct(t *testing.T) {
	p := pluginTestData.NewPlugin("Elasticsearch", Url)

	err0 := p.SetParam(getParam())
	p.SetEventType(pluginTestData.INSERT)
	if err0 != nil {
		t.Fatal(err0)
	}

	var n uint = 10000
	err := p.DoTestStart(n)

	if err != nil {
		t.Fatal(err)
	} else {
		t.Log("test success")
	}
}

// TestCommitAndCheckData2 这个通不过， 不支持修改字段类型
func TestCommitAndCheckData2(t *testing.T) {
	testBefore()
	initSyncParam()
	event := pluginTestData.NewEvent()
	event.SetNoUint64(true)

	eventData := event.GetTestInsertData()
	eventData.Rows[0]["testint"] = "1334　" // 这个通不过， 不支持修改字段类型
	conn.Insert(eventData, false)
	_, _, err2 := conn.TimeOutCommit()
	if err2 != nil {
		t.Fatal(err2)
	}

	m := eventData.Rows[len(eventData.Rows)-1]
	// time.Sleep(1 * time.Second)
	// c := NewClickHouseDBConn(url)
	// dataList := c.GetTableDataList(eventData.SchemaName, eventData.TableName, "id="+fmt.Sprint(m["id"]))
	dataList, _ := myConn.client.Get().Index(myConn.p.EsIndexName).Id(fmt.Sprint(eventData.Rows[0]["id"])).Do(context.Background())

	if len(dataList.Fields) == 0 {
		t.Fatal("select data len == 0")
	}

	resultData := make(map[string][]string, 0)
	resultData["ok"] = make([]string, 0)
	resultData["error"] = make([]string, 0)

	checkDataRight(m, dataList.Fields, resultData)

	// for _, v := range resultData["ok"] {
	// 	t.Log(v)
	// }

	for _, v := range resultData["error"] {
		t.Error(v)
	}

	if len(resultData["error"]) == 0 {
		t.Log("test over;", "data is all right")
	} else {
		t.Error("test over;", " some data is error")
	}

}

func TestNewTableData(t *testing.T) {
	c := NewTableData()
	if c.CommitData[0] == nil {
		t.Log("test frist 0 index is nil")
	}
	c.CommitData = c.CommitData[1:]
	t.Log("success")
}

func TestConn_GetVersion(t *testing.T) {
	tests := []struct {
		name        string
		wantVersion string
	}{
		// TODO: Add test cases.
		{name: ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			This := &Conn{status: "close", err: fmt.Errorf("close")}
			This.SetOption(&Url, nil)
			gotVersion, _ := This.GetVersion()
			log.Println(gotVersion)
		})
	}
}

func TestConn_Mget(t *testing.T) {
	testBefore()
	initSyncParam()
	event := pluginTestData.NewEvent()
	event.SetNoUint64(true)

	eventData := event.GetTestInsertData()
	eventData.Rows[0]["testint"] = "1334　" // 这个通不过， 不支持修改字段类型
	conn.Insert(eventData, false)
	eventData2 := event.GetTestInsertData()
	conn.Insert(eventData2, false)
	_, _, err2 := conn.TimeOutCommit()
	if err2 != nil {
		t.Fatal(err2)
	}
	q1 := &elastic.MultiGetItem{}
	q1.Index(myConn.p.EsIndexName).Id(fmt.Sprint(eventData.Rows[0]["id"]))
	q2 := &elastic.MultiGetItem{}
	q2.Index(myConn.p.EsIndexName).Id(fmt.Sprint(eventData2.Rows[0]["id"]))
	MgetResponse, err := myConn.client.MultiGet().Add(q1, q2).Do(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	for _, v := range MgetResponse.Docs {
		t.Log(string(v.Source))
		for key, val := range v.Fields {
			t.Log(key, val)
		}
	}
}

func checkDataRight(m map[string]interface{}, destMap map[string]interface{}, resultData map[string][]string) {
	for columnName, v := range destMap {
		if _, ok := m[columnName]; !ok {
			resultData["error"] = append(resultData["error"], fmt.Sprint(columnName, " not exsit"))
		}
		var result bool = false
		switch m[columnName].(type) {
		case []string:
			// sourceData := strings.Replace(strings.Trim(fmt.Sprint(m[columnName]), "[]"), " ", ",", -1)
			if fmt.Sprint(v) == fmt.Sprint(fmt.Sprint(m[columnName])) {
				result = true
			}
			break
		case float32, float64:
			//假如都是浮点数，因为精度问题，都先转成string 再转成 float64 ，再做差值处理，小于0.05 就算正常了
			floatDest, _ := strconv.ParseFloat(fmt.Sprint(v), 64)
			floatSource, _ := strconv.ParseFloat(fmt.Sprint(m[columnName]), 64)
			if math.Abs(floatDest-floatSource) < 0.05 {
				result = true
			}
			break

		default:
			switch v.(type) { // dest
			//这里需要去一次空格对比,因为有可能源是 带空格的字符串
			case int, uint, int8, uint8, int16, uint16, int32, uint32, int64, uint64:
				a, _ := strconv.Atoi(strings.Trim(fmt.Sprint(v), " "))
				b, _ := strconv.Atoi(strings.Trim(fmt.Sprint(m[columnName]), " "))

				if a == b {
					result = true
				}
				break
			case float32, float64:
				//假如都是浮点数，因为精度问题，都先转成string 再转成 float64 ，再做差值处理，小于0.05 就算正常了
				floatDest, _ := strconv.ParseFloat(fmt.Sprint(v), 64)
				floatSource, _ := strconv.ParseFloat(fmt.Sprint(m[columnName]), 64)
				if math.Abs(floatDest-floatSource) < 0.05 {
					result = true
				}
				break
			case time.Time:
				// 这里用包括关系 ，也是因为 es 读出来的时候，date和datetime类型都转成了time.Time 类型了
				descTime := fmt.Sprint(v.(time.Time).Format("2006-01-02 15:04:05"))
				if descTime == fmt.Sprint(m[columnName]) || strings.Index(descTime, fmt.Sprint(m[columnName])) == 0 {
					result = true
				}
				break
			case map[string]interface{}:

				if fmt.Sprint(v)[:10] == fmt.Sprint(m[columnName])[:10] {
					result = true
				} else {
					fmt.Println("result = false, v,m[columnName] ", v, m[columnName], reflect.TypeOf(v), reflect.TypeOf(m[columnName]))
				}
				break
			default:

				if fmt.Sprint(v) == fmt.Sprint(m[columnName]) {
					result = true
				} else {
					fmt.Println("result = false, v,m[columnName] ", fmt.Sprint(v), fmt.Sprint(m[columnName]), reflect.TypeOf(v), reflect.TypeOf(m[columnName]))
				}
				break
			}

			break
		}
		if result {
			resultData["ok"] = append(resultData["ok"], fmt.Sprint(columnName, " dest: ", v, "(", reflect.TypeOf(v), ")", " == ", m[columnName], "(", reflect.TypeOf(m[columnName]), ")"))
		} else {
			resultData["error"] = append(resultData["error"], fmt.Sprint(columnName, " dest: ", v, "(", reflect.TypeOf(v), ")", " != ", m[columnName], "(", reflect.TypeOf(m[columnName]), ")"))
		}
	}
}
