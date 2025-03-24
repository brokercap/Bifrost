//go:build integration
// +build integration

package src_test

import (
	pluginDriver "github.com/brokercap/Bifrost/plugin/driver"
	MyPlugin "github.com/brokercap/Bifrost/plugin/rabbitmq/src"
	"github.com/brokercap/Bifrost/sdk/pluginTestData"
	"github.com/streadway/amqp"
	"log"
	"testing"
	"time"
)

var TestUrl = "amqp://admin:admin123@10.40.6.89:5672/testvhost"

func testInitRabbitMQConn() (*amqp.Connection, error) {
	rabbitmqConn, err := amqp.Dial(TestUrl)
	return rabbitmqConn, err
}

func testGetParam(Confirm bool) map[string]interface{} {
	type Queue struct {
		Name       string
		Durable    bool
		AutoDelete bool
	}

	type Exchange struct {
		Name       string
		Type       string
		Durable    bool
		AutoDelete bool
	}

	var QueueInfo Queue
	var ExchangeInfo Exchange

	var RoutingKey string
	QueueInfo = Queue{
		Name:       "rabbitmq_test_{$SchemaName}_{$TableName}",
		Durable:    true,
		AutoDelete: false,
	}

	ExchangeInfo = Exchange{
		Name:       QueueInfo.Name,
		Durable:    true,
		AutoDelete: false,
		Type:       "direct",
	}

	RoutingKey = QueueInfo.Name

	rabbitmqPluginPamram := make(map[string]interface{}, 0)
	rabbitmqPluginPamram["Queue"] = QueueInfo
	rabbitmqPluginPamram["Exchange"] = ExchangeInfo
	rabbitmqPluginPamram["Confirm"] = Confirm
	rabbitmqPluginPamram["Persistent"] = true

	rabbitmqPluginPamram["RoutingKey"] = RoutingKey
	rabbitmqPluginPamram["Expir"] = 0
	rabbitmqPluginPamram["Declare"] = true

	return rabbitmqPluginPamram
}

func TestCheckData(t *testing.T) {
	conn := MyPlugin.NewConn()
	conn.SetOption(&TestUrl, nil)
	conn.Open()
	conn.SetParam(testGetParam(true))

	e := pluginTestData.NewEvent()

	Schema1 := "bifrost_test1"
	TableName1 := "binlog_field_test1"

	Schema2 := "bifrost_test2"
	TableName2 := "binlog_field_test2"

	e.SetSchema(Schema1)
	e.SetTable(TableName1)

	t.Log("insert test start")
	test1List := make([]*pluginDriver.PluginDataType, 0)
	test1List = append(test1List, e.GetTestInsertData())
	test1List = append(test1List, e.GetTestInsertData())

	for _, v := range test1List {
		conn.Insert(v, false)
	}

	t.Log("insert test over")

	e.SetSchema(Schema2)
	e.SetTable(TableName2)

	t.Log("update test start")

	testList2 := make([]*pluginDriver.PluginDataType, 0)
	testList2 = append(testList2, e.GetTestUpdateData())
	testList2 = append(testList2, e.GetTestUpdateData())

	for _, v := range testList2 {
		conn.Update(v, false)
	}

	t.Log("update test over")

	rabbitmqConn, err := testInitRabbitMQConn()
	if err != nil {
		t.Fatal(err)
	}

	var cosume = func(QueueName string, data []*pluginDriver.PluginDataType) {
		channel1, err1 := rabbitmqConn.Channel()
		if err1 != nil {
			t.Fatal(err1)
		}
		msgs, err := channel1.Consume(
			QueueName, // queue
			"",        // consumer
			false,     // auto ack
			false,     // exclusive
			false,     // no local
			false,     // no wait
			nil,       // args
		)
		if err == nil {
		Loop:
			for i := 0; i < len(data); i++ {
				select {
				case d := <-msgs:
					log.Println("src：", data[i].Rows[len(data[i].Rows)-1])
					log.Println(string(d.Body))
					checkResult, err := e.CheckData2(data[i].Rows[len(data[i].Rows)-1], string(d.Body))
					d.Ack(false)
					if err != nil {

						t.Error(QueueName, err)
						continue
					}

					for _, v := range checkResult["ok"] {
						t.Log(QueueName, v)
					}

					for _, v := range checkResult["error"] {
						t.Error(QueueName, v)
					}

					break
				case <-time.After(time.Duration(10) * time.Second):
					break Loop
				}
			}
		} else {
			t.Error(err)
		}
		channel1.Close()
	}

	QueueName1 := "rabbitmq_test_" + Schema1 + "_" + TableName1

	t.Log("insert chekcdata test start")
	cosume(QueueName1, test1List)

	t.Log("insert chekcdata test over")

	QueueName2 := "rabbitmq_test_" + Schema2 + "_" + TableName2

	t.Log("update chekcdata test start")

	cosume(QueueName2, testList2)

	t.Log("update chekcdata test over")

	t.Log("test over")

}

func TestJosn(t *testing.T) {

}
