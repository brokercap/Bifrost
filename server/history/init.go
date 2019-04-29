package history

import (
	"fmt"
	"sync"
	"strconv"
	"github.com/jc3wish/Bifrost/Bristol/mysql"
	"database/sql/driver"
	"github.com/jc3wish/Bifrost/util/dataType"
	"strings"
	"log"
)

type HistoryProperty struct {
	ThreadNum			int      // 协程数量,每个协程一个连接
	ThreadCountPer		int		   // 协程每次最多处理多少条数据
}

const (
	CLOSE = 0
	RUNNING = 1
	STOP = 2
	HALFWAY = 3
)

type ThreadStatus struct {
	Num					int
	Error				error
	NowStartI			int     // 当前执行第几条
}

type History struct {
	sync.RWMutex
	DbName 				string
	SchemaName			string
	TableName			string
	Property			HistoryProperty
	Status				int //close ,running,stop, halfway
	NowStartI			int //当前第几条数据
	ThreadPool			[]*ThreadStatus
	threadResultChan	chan int
	Fields				[]TableStruct
	Uri					string
}

func (This *History) Start() error {
	log.Println("history start",This.SchemaName,This.TableName)
	This.Lock()
	if This.Status == RUNNING{
		This.Unlock()
		return fmt.Errorf("running had")
	}
	This.Status = RUNNING
	This.Unlock()
	This.Fields = make([]TableStruct,0)
	This.ThreadPool = make([]*ThreadStatus,This.Property.ThreadNum)
	This.threadResultChan = make(chan int,1)
	for i:=1;i<=This.Property.ThreadNum;i++{
		go This.threadStart(i)
	}
	c:=0
	for{
		if c == This.Property.ThreadNum{
			break
		}
		i := <- This.threadResultChan
		log.Println("thread over:",i)
		c++
	}
	This.Status = STOP
	return nil
}

func (This *History) getMetaInfo(db mysql.MysqlConnection)  {
	This.Lock()
	defer This.Unlock()
	if len(This.Fields) > 0{
		return
	}
	This.Fields = GetSchemaTableFieldList(db,This.SchemaName,This.TableName)
	return
}

func (This *History) threadStart(i int)  {
	log.Println("threadStart start:",i,This.SchemaName,This.TableName)
	defer func() {
		This.threadResultChan <- i
	}()
	db := DBConnect(This.Uri)
	This.getMetaInfo(db)
	if len(This.Fields) == 0{
		log.Println("Fields empty",This.SchemaName,This.TableName)
		return
	}
	var start int

	for {
		This.Lock()
		start = This.NowStartI
		This.NowStartI += This.Property.ThreadCountPer
		This.Unlock()

		sql := "select * from " + This.SchemaName + "." + This.TableName + " LIMIT " + strconv.Itoa(start) + "," + strconv.Itoa(This.Property.ThreadCountPer)
		stmt, err := db.Prepare(sql)
		if err != nil {
			return
		}
		p := make([]driver.Value, 0)
		rows, err := stmt.Query(p)
		n := len(This.Fields)
		m := make(map[string]interface{}, n)
		for {
			dest := make([]driver.Value, n, n)
			err := rows.Next(dest)
			if err != nil {
				break
			}

			for i, v := range This.Fields {
				switch v.COLUMN_TYPE {
				case "set":
					s :=  string(dest[i].([]byte))
					m[v.COLUMN_NAME] = strings.Split(s, ",")
					break
				default:
					m[v.COLUMN_NAME], _ = dataType.TransferDataType(dest[i].([]byte), v.ToDataType)
					break
				}
				log.Println(v.COLUMN_NAME,m[v.COLUMN_NAME])
			}
		}

		log.Println("m:",m)
		return
	}
}