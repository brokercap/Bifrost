package driver

import (
	"fmt"
	"strings"
)

func (c *PluginDataType) ToTableMapObject() (data map[string]interface{}, err error) {
	data = make(map[string]interface{}, 0)
	if len(c.Rows) > 0 {
		for k, v := range c.Rows[len(c.Rows)-1] {
			switch v.(type) {
			case int, uint, uint64, int64, uint32, int32, uint16, int16, uint8, int8:
				data[k] = fmt.Sprintf("%d", v)
			case float64, float32:
				data[k] = fmt.Sprintf("%f", v)
			case bool:
				data[k] = fmt.Sprint(v)
			default:
				data[k] = v
			}
		}
	} else {
		data["bifrost_query"] = c.Query
	}
	data["binlog_event_type"] = c.EventType
	data["bifrost_pri"] = strings.Join(c.Pri, ",")
	data["bifrost_database"] = c.SchemaName
	data["bifrost_table"] = c.TableName
	return
}
