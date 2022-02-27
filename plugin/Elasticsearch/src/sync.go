package src

import (
	"context"
	"fmt"
	"log"
	"net/http"

	"github.com/juju/errors"

	pluginDriver "github.com/brokercap/Bifrost/plugin/driver"
	elastic "github.com/olivere/elastic/v7"
)

// commitNormal commitNormal
func (This *Conn) commitNormal(list []*pluginDriver.PluginDataType, n int) (errData *pluginDriver.PluginDataType, err error) {
	reqs := make([]elastic.BulkableRequest, 0, len(list))
	var normalFun = func(v *pluginDriver.PluginDataType, reqs1 []elastic.BulkableRequest) {
		var reqs2 []elastic.BulkableRequest
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
	if err = This.sendBulkRequests(reqs); err != nil {
		log.Printf("do ES bulk err %v, close sync", err)
		return
	}
	return
}

// makeInsertRequest makeInsertRequest
func (This *Conn) makeInsertRequest(rows []map[string]interface{}) ([]elastic.BulkableRequest, error) {
	reqs := make([]elastic.BulkableRequest, 0, len(rows))
	for _, values := range rows {
		id, err := This.getDocID(values)
		if err != nil {
			return nil, errors.Trace(err)
		}
		req := elastic.NewBulkUpdateRequest().
			Index(This.p.EsIndexName).
			RetryOnConflict(This.esServerInfo.RetryCount).
			Id(id).
			Doc(values).DocAsUpsert(true).
			Upsert(values).Type("_doc")

		reqs = append(reqs, req)
	}
	return reqs, nil
	//return This.makeRequest(ActionIndex, rows)
}

// makeDeleteRequest makeDeleteRequest
func (This *Conn) makeDeleteRequest(rows []map[string]interface{}) ([]elastic.BulkableRequest, error) {
	reqs := make([]elastic.BulkableRequest, 0, len(rows))
	for _, values := range rows {
		id, err := This.getDocID(values)
		if err != nil {
			return nil, errors.Trace(err)
		}
		req := elastic.NewBulkDeleteRequest().Type("_doc").
			Index(This.p.EsIndexName).
			Id(id)
		reqs = append(reqs, req)
	}
	return reqs, nil
}

// makeUpdateRequest makeUpdateRequest
func (This *Conn) makeUpdateRequest(rows []map[string]interface{}) ([]elastic.BulkableRequest, error) {
	if len(rows)%2 != 0 {
		return nil, errors.Errorf("invalid update rows event, must have 2x rows, but %d", len(rows))
	}
	reqs := make([]elastic.BulkableRequest, 0, len(rows))
	for i := 0; i < len(rows); i += 2 {
		afterID, err := This.getDocID(rows[i+1])
		if err != nil {
			return nil, errors.Trace(err)
		}
		req := elastic.NewBulkUpdateRequest().
			Index(This.p.EsIndexName).
			RetryOnConflict(This.esServerInfo.RetryCount).
			Id(afterID).
			Doc(rows[i+1]).DocAsUpsert(true).
			Upsert(rows[i+1]).Type("_doc")

		reqs = append(reqs, req)
	}
	return reqs, nil
}

func (This *Conn) getDocID(row map[string]interface{}) (id string, err error) {
	for _, key := range This.p.primaryKeys {
		if _, ok := row[key]; ok {
			id = fmt.Sprint(row[key])
		} else {
			return "", fmt.Errorf("key:" + key + " no exsit")
		}
	}
	return
}

func (output *Conn) sendBulkRequests(reqs []elastic.BulkableRequest) error {
	if len(reqs) == 0 {
		return nil
	}
	bulkRequest := output.client.Bulk()
	bulkRequest.Add(reqs...)
	bulkResponse, err := bulkRequest.Do(context.Background())
	if err != nil {
		return errors.Trace(err)
	}

	for _, item := range bulkResponse.Items {
		for action, result := range item {
			if output.isSuccessful(result, action) {
				// tags: [pipelineName, index, action(index/create/delete/update), status(200/400)].
				// indices created in 6.x only allow a single-type per index, so we don't need the type as a tag.
				var status int
				if result.Status == http.StatusBadRequest {
					//printJsonEncodef("[output_elasticsearch] The remote server returned an error: (400) Bad request, index: %s, details: %s.", result.Index, marshalError(result.Error))
					log.Printf("[output_elasticsearch] The remote server returned an error: (400) Bad request, index: %s, action:%s ,status:%d ,details: %T.", result.Index, action, status, result.Error)
					status = http.StatusBadRequest
				} else {
					// 200/201/404(delete) -> 200 because the request is successful
					status = http.StatusOK
				}
			} else if result.Status == http.StatusTooManyRequests {
				// when the server returns 429, it must be that all requests have failed.
				return errors.Errorf("[output_elasticsearch] The remote server returned an error: (429) Too Many Requests.")
			} else {
				return errors.Errorf("[output_elasticsearch] Received an error from server, status: [%d], index: %s, action:%s ,status:%d ,details: %+v.", result.Status, result.Index, action, result.Status, result.Error)
			}
		}
	}
	return nil
}

func (This *Conn) isSuccessful(result *elastic.BulkResponseItem, action string) bool {
	return (result.Status >= 200 && result.Status <= 299) ||
		(result.Status == http.StatusNotFound && action == "delete") || // delete but not found, just ignore it.
		(result.Status == http.StatusBadRequest && !This.p.BifrostMustBeSuccess) // ignore index not found, parse error, etc.
}
