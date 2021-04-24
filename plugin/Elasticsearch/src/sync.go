package src

import (
	"fmt"
	"sync/atomic"

	"log"

	"github.com/brokercap/Bifrost/plugin/Elasticsearch/src/elastic"
	"github.com/juju/errors"

	pluginDriver "github.com/brokercap/Bifrost/plugin/driver"
)

const (
	fieldTypeList = "list"
	// for the mysql int type to es date type
	// set the [rule.field] created_time = ",date"
	fieldTypeDate = "date"
)

const mysqlDateFormat = "2006-01-02"

// See http://www.elasticsearch.org/guide/en/elasticsearch/guide/current/bulk.html
const (
	ActionCreate = "create"
	ActionUpdate = "update"
	ActionDelete = "delete"
	ActionIndex  = "index"
)

// The action name for sync.
const (
	UpdateAction = "update"
	InsertAction = "insert"
	DeleteAction = "delete"
)

// commitNormal commitNormal
func (This *Conn) commitNormal(list []*pluginDriver.PluginDataType, n int) (errData *pluginDriver.PluginDataType,err error) {
	reqs := make([]*elastic.BulkRequest, 0, len(list))

	var normalFun = func(v *pluginDriver.PluginDataType, reqs1 []*elastic.BulkRequest) {
		reqs2 := make([]*elastic.BulkRequest, 0, len(list))

		switch v.EventType {
		case "insert":
			reqs2, _ = This.makeInsertRequest(v.Rows)
			break
		case "update":
			reqs2, _ = This.makeUpdateRequest(v.Rows)

			break
		case "delete":
			reqs2, _ = This.makeDeleteRequest(v.Rows)
			break
		default:
			break
		}
		// log.Println("v.EventType:", v.EventType)

		reqs = append(reqs, reqs2...)
	}

	for i := 0; i <= n-1; i++ {
		v := list[i]
		normalFun(v, reqs)
	}
	// log.Println("reqs:", g.Export(reqs))

	for !This.p.hadMapping[This.p.EsIndexName] {
		This.doCreateMapping()
	}
	// TODO: retry some times?
	if err = This.doBulkSync(reqs); err != nil {
		log.Printf("do ES bulk err %v, close sync", err)
		return
	}
	return
}

// makeRequest for insert and delete
func (This *Conn) makeRequest(action string, rows []map[string]interface{}) ([]*elastic.BulkRequest, error) {
	reqs := make([]*elastic.BulkRequest, 0, len(rows))

	for _, values := range rows {
		id, err := This.getDocID(values)
		if err != nil {
			return nil, errors.Trace(err)
		}

		req := &elastic.BulkRequest{Index: This.p.EsIndexName, ID: id}

		if action == DeleteAction {
			req.Action = ActionDelete
		} else {
			req.Action = ActionIndex
			req.Data = values
		}

		reqs = append(reqs, req)
	}

	return reqs, nil
}

// makeInsertRequest makeInsertRequest
func (This *Conn) makeInsertRequest(rows []map[string]interface{}) ([]*elastic.BulkRequest, error) {
	return This.makeRequest(ActionIndex, rows)
}

// makeDeleteRequest makeDeleteRequest
func (This *Conn) makeDeleteRequest(rows []map[string]interface{}) ([]*elastic.BulkRequest, error) {
	return This.makeRequest(DeleteAction, rows)
}

// makeUpdateRequest makeUpdateRequest
func (This *Conn) makeUpdateRequest(rows []map[string]interface{}) ([]*elastic.BulkRequest, error) {
	if len(rows)%2 != 0 {
		return nil, errors.Errorf("invalid update rows event, must have 2x rows, but %d", len(rows))
	}

	reqs := make([]*elastic.BulkRequest, 0, len(rows))

	for i := 0; i < len(rows); i += 2 {

		afterID, err := This.getDocID(rows[i+1])

		if err != nil {
			return nil, errors.Trace(err)
		}

		req := &elastic.BulkRequest{Index: This.p.EsIndexName, ID: afterID}
		req.Action = ActionIndex
		req.Data = rows[i+1]

		reqs = append(reqs, req)

	}

	return reqs, nil
}

func (This *Conn) getDocID(row map[string]interface{}) (id interface{}, err error) {
	for _, key := range This.p.primaryKeys {
		if _, ok := row[key]; ok {
			id = row[key]
		} else {
			return nil, fmt.Errorf("key:" + key + " no exsit")
		}
	}
	return
}

func (This *Conn) AddRetryCount() (res bool) {
	for {
		TempInt := atomic.LoadInt64(&This.RetryCount)
		Result := atomic.CompareAndSwapInt64(&This.RetryCount, TempInt, TempInt+1)
		if Result == true {
			fmt.Println(TempInt, " Try to CAS: ", Result)
			if TempInt > 2 { // 2
				res = false
				This.RetryCount = 0
				return
			}
			res = true
			break
		} else {
			fmt.Println(TempInt, " Try to CAS: ", Result)
		}
	}
	return
}

// doBulkSync doBulkSync
func (This *Conn) doBulkSync(reqs []*elastic.BulkRequest) error {

	for ii := 0; ii < 5; ii++ {

		if len(reqs) == 0 {
			return nil
		}

		if resp, err := This.conn.Bulk(reqs); err != nil {
			log.Printf("sync docs err %v \n", err)
			return errors.Trace(err)
		} else if resp.Code/100 == 2 || resp.Errors {
			reqs1 := []*elastic.BulkRequest{}
			reqsMap := map[interface{}]*elastic.BulkRequest{}
			for i := range reqs {
				reqsMap[fmt.Sprint(reqs[i].ID)] = reqs[i]
			}
			for i := 0; i < len(resp.Items); i++ {
				for action, item := range resp.Items[i] {
					if len(item.Error) > 0 {
						log.Printf("%s index: %s, type: %s, id: \"%s\", status: %d, error: %s\n",
							action, item.Index, item.Type, item.ID, item.Status, item.Error)
						reqs1 = append(reqs1, reqsMap[item.ID])
						// g.Dump("reqsMap[item.ID]", reqsMap[item.ID], item.ID)
					}
				}
			}
			reqs = reqs1
		}
	}

	return nil
}
