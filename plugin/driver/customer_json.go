package driver

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"
)

type PluginCustomerJsonDataKey2Row struct {
	Name string
	Path []string
}

type PluginDataCustomerJson struct {
	msgData map[string]interface{}

	key2row           []PluginCustomerJsonDataKey2Row
	databasePath      []string
	tablePath         []string
	updateNewDataPath []string
	UpdateOldDataPath []string
	insertDataPath    []string
	deleteDataPath    []string

	pksPath []string

	eventTypePath      []string
	eventTypeValInsert string
	eventTypeValSelect string
	eventTypeValUpdate string
	eventTypeValDelete string
}

func NewPluginDataCustomerJson() (*PluginDataCustomerJson, error) {
	return &PluginDataCustomerJson{
		eventTypeValInsert: "insert",
		eventTypeValSelect: "insert",
		eventTypeValUpdate: "update",
		eventTypeValDelete: "delete",
	}, nil
}

func (c *PluginDataCustomerJson) Decoder(content []byte) error {
	var data map[string]interface{}
	decocer := json.NewDecoder(bytes.NewBuffer(content))
	decocer.UseNumber()
	err := decocer.Decode(&data)
	c.msgData = data
	return err
}

func (c *PluginDataCustomerJson) SetKey2Row(key2row []PluginCustomerJsonDataKey2Row) {
	c.key2row = key2row
}

func (c *PluginDataCustomerJson) SetDatabasePath(path []string) {
	c.databasePath = path
}

func (c *PluginDataCustomerJson) SetTablePath(path []string) {
	c.tablePath = path
}

func (c *PluginDataCustomerJson) SetInsertDataPath(path []string) {
	c.insertDataPath = path
}

func (c *PluginDataCustomerJson) SetUpdateNewDataPath(path []string) {
	c.updateNewDataPath = path
}

func (c *PluginDataCustomerJson) SetUpdateOldDataPath(path []string) {
	c.UpdateOldDataPath = path
}

func (c *PluginDataCustomerJson) SetDeleteDataPath(path []string) {
	c.deleteDataPath = path
}

func (c *PluginDataCustomerJson) SetPksPath(path []string) {
	c.pksPath = path
}

func (c *PluginDataCustomerJson) SetEventTypePath(path []string) {
	c.eventTypePath = path
}

func (c *PluginDataCustomerJson) SetEventTypeValInsert(eventName string) {
	c.eventTypeValInsert = eventName
}

func (c *PluginDataCustomerJson) SetEventTypeValSelect(eventName string) {
	c.eventTypeValSelect = eventName
}

func (c *PluginDataCustomerJson) SetEventTypeValUpdate(eventName string) {
	c.eventTypeValUpdate = eventName
}

func (c *PluginDataCustomerJson) SetEventTypeValDelete(eventName string) {
	c.eventTypeValDelete = eventName
}

func (c *PluginDataCustomerJson) GetInterfaceData(path []string) (val interface{}) {
	n := len(path)
	if n == 0 {
		return nil
	}
	if n == 1 {
		val = c.msgData[path[0]]
		return
	}
	var tmpMsgData map[string]interface{}
	tmpMsgData = c.msgData
	for i, v := range path {
		tmp := tmpMsgData[v]
		val = tmp
		switch tmp.(type) {
		case map[string]interface{}:
			tmpMsgData = tmp.(map[string]interface{})
		default:
			if n+1 < n {
				panic(fmt.Sprintf("%s is not map[string]interface{}", strings.Join(path[:i], ".")))
			}
		}
	}
	return
}

func (c *PluginDataCustomerJson) GetMapData(path []string) map[string]interface{} {
	if path == nil {
		return nil
	}
	var tmpMsgData map[string]interface{}
	tmpMsgData = c.msgData
	for _, v := range path {
		tmpMsgData = tmpMsgData[v].(map[string]interface{})
	}
	return tmpMsgData
}

func (c *PluginDataCustomerJson) GetPksData() (pks []string) {
	if len(c.pksPath) <= 0 {
		return nil
	}
	pksObj := c.GetInterfaceData(c.pksPath)
	if pksObj == nil {
		return nil
	}
	switch pksObj.(type) {
	case map[string]interface{}:
		for key, _ := range pksObj.(map[string]interface{}) {
			pks = append(pks, key)
		}
		break
	case []interface{}:
		for _, key := range pksObj.([]interface{}) {
			pks = append(pks, fmt.Sprint(key))
		}
		break
	default:
		pks = append(pks, fmt.Sprint(pksObj))
		break
	}
	return pks
}

func (c *PluginDataCustomerJson) GetEventType() string {
	eventType := c.GetInterfaceData(c.eventTypePath)
	return fmt.Sprint(eventType)
}

func (c *PluginDataCustomerJson) ToBifrostOutputPluginData() (data *PluginDataType) {
	eventType := c.GetEventType()
	var rows []map[string]interface{}
	switch eventType {
	case c.eventTypeValInsert, c.eventTypeValSelect:
		rows = c.ToBifrostInsertRows()
		break
	case c.eventTypeValUpdate:
		rows = c.ToBifrostUpdateRows()
		break
	case c.eventTypeValDelete:
		rows = c.ToBifrostDeleteRows()
		break
	default:
		return nil
	}
	data = &PluginDataType{
		EventType:     eventType,
		Rows:          rows,
		Pri:           c.GetPksData(),
		ColumnMapping: nil,
	}
	c.ToKey2Row(data)
	return data
}

func (c *PluginDataCustomerJson) ToBifrostInsertRows() (rows []map[string]interface{}) {
	rows = append(rows, c.GetMapData(c.insertDataPath))
	return rows
}

func (c *PluginDataCustomerJson) ToBifrostUpdateRows() (rows []map[string]interface{}) {
	newData := c.GetMapData(c.updateNewDataPath)
	oldData := c.GetMapData(c.UpdateOldDataPath)
	if oldData == nil {
		oldData = newData
	}
	rows = append(rows, oldData, newData)
	return rows
}

func (c *PluginDataCustomerJson) ToBifrostDeleteRows() (rows []map[string]interface{}) {
	rows = append(rows, c.GetMapData(c.deleteDataPath))
	return rows
}

func (c *PluginDataCustomerJson) ToKey2Row(data *PluginDataType) {
	if len(c.key2row) == 0 {
		return
	}
	for _, v := range c.key2row {
		for i := range data.Rows {
			data.Rows[i][v.Name] = c.GetInterfaceData(v.Path)
		}
	}
}
