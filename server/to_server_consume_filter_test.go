package server

import (
	"errors"
	pluginDriver "github.com/brokercap/Bifrost/plugin/driver"
	"testing"
)

type mockBasePluginDriver struct{}

func (m *mockBasePluginDriver) SetOption(uri *string, param map[string]interface{}) {}

func (m *mockBasePluginDriver) Open() error {
	return nil
}

func (m *mockBasePluginDriver) Close() bool {
	return true
}

func (m *mockBasePluginDriver) GetUriExample() string {
	return ""
}

func (m *mockBasePluginDriver) CheckUri() error {
	return nil
}

func (m *mockBasePluginDriver) Insert(data *pluginDriver.PluginDataType, retry bool) (*pluginDriver.PluginDataType, *pluginDriver.PluginDataType, error) {
	return data, nil, nil
}

func (m *mockBasePluginDriver) Update(data *pluginDriver.PluginDataType, retry bool) (*pluginDriver.PluginDataType, *pluginDriver.PluginDataType, error) {
	return data, nil, nil
}

func (m *mockBasePluginDriver) Del(data *pluginDriver.PluginDataType, retry bool) (*pluginDriver.PluginDataType, *pluginDriver.PluginDataType, error) {
	return data, nil, nil
}

func (m *mockBasePluginDriver) Query(data *pluginDriver.PluginDataType, retry bool) (*pluginDriver.PluginDataType, *pluginDriver.PluginDataType, error) {
	return data, nil, nil
}

func (m *mockBasePluginDriver) Commit(data *pluginDriver.PluginDataType, retry bool) (*pluginDriver.PluginDataType, *pluginDriver.PluginDataType, error) {
	return data, nil, nil
}

func (m *mockBasePluginDriver) SetParam(p interface{}) (interface{}, error) {
	return p, nil
}

func (m *mockBasePluginDriver) TimeOutCommit() (*pluginDriver.PluginDataType, *pluginDriver.PluginDataType, error) {
	return nil, nil, nil
}

func (m *mockBasePluginDriver) Skip(data *pluginDriver.PluginDataType) error {
	return nil
}

type mockFilterPluginDriver struct {
	mockBasePluginDriver
	filterFn func(data *pluginDriver.PluginDataType, retry bool) (*pluginDriver.PluginDataType, bool, error)
}

func (m *mockFilterPluginDriver) Filter(data *pluginDriver.PluginDataType, retry bool) (*pluginDriver.PluginDataType, bool, error) {
	return m.filterFn(data, retry)
}

func TestToServerApplyPluginFilterPassThrough(t *testing.T) {
	s := &ToServer{}
	input := &pluginDriver.PluginDataType{
		EventType: "insert",
		Rows: []map[string]interface{}{
			{
				"id":   1,
				"name": "alice",
			},
		},
	}
	output, keep, err := s.applyPluginFilter(&mockBasePluginDriver{}, input, false)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !keep {
		t.Fatalf("expected keep=true")
	}
	if output != input {
		t.Fatalf("expected same data pointer for pass-through")
	}
}

func TestToServerApplyPluginFilterDynamicFields(t *testing.T) {
	s := &ToServer{}
	input := &pluginDriver.PluginDataType{
		EventType: "insert",
		Rows: []map[string]interface{}{
			{
				"id":         1,
				"name":       "alice",
				"drop_field": "x",
			},
		},
	}
	driver := &mockFilterPluginDriver{
		filterFn: func(data *pluginDriver.PluginDataType, retry bool) (*pluginDriver.PluginDataType, bool, error) {
			row := map[string]interface{}{
				"id":        data.Rows[0]["id"],
				"name":      data.Rows[0]["name"],
				"new_field": "from-filter",
			}
			newData := *data
			newData.Rows = []map[string]interface{}{row}
			return &newData, true, nil
		},
	}
	output, keep, err := s.applyPluginFilter(driver, input, false)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !keep {
		t.Fatalf("expected keep=true")
	}
	if _, ok := output.Rows[0]["drop_field"]; ok {
		t.Fatalf("drop_field should be removed by filter plugin")
	}
	if output.Rows[0]["new_field"] != "from-filter" {
		t.Fatalf("new_field not added by filter plugin")
	}
}

func TestToServerApplyPluginFilterDropEvent(t *testing.T) {
	s := &ToServer{}
	input := &pluginDriver.PluginDataType{
		EventType: "insert",
		Rows:      []map[string]interface{}{{"id": 1}},
	}
	driver := &mockFilterPluginDriver{
		filterFn: func(data *pluginDriver.PluginDataType, retry bool) (*pluginDriver.PluginDataType, bool, error) {
			return nil, false, nil
		},
	}
	_, keep, err := s.applyPluginFilter(driver, input, false)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if keep {
		t.Fatalf("expected keep=false")
	}
}

func TestToServerApplyPluginFilterKeepTrueButNilData(t *testing.T) {
	s := &ToServer{}
	input := &pluginDriver.PluginDataType{
		EventType: "insert",
		Rows:      []map[string]interface{}{{"id": 1}},
	}
	driver := &mockFilterPluginDriver{
		filterFn: func(data *pluginDriver.PluginDataType, retry bool) (*pluginDriver.PluginDataType, bool, error) {
			return nil, true, nil
		},
	}
	_, _, err := s.applyPluginFilter(driver, input, false)
	if err == nil {
		t.Fatalf("expected error when keep=true but data=nil")
	}
}

func TestToServerApplyPluginFilterReturnError(t *testing.T) {
	s := &ToServer{}
	input := &pluginDriver.PluginDataType{
		EventType: "insert",
		Rows:      []map[string]interface{}{{"id": 1}},
	}
	driver := &mockFilterPluginDriver{
		filterFn: func(data *pluginDriver.PluginDataType, retry bool) (*pluginDriver.PluginDataType, bool, error) {
			return data, true, errors.New("filter failed")
		},
	}
	_, _, err := s.applyPluginFilter(driver, input, false)
	if err == nil {
		t.Fatalf("expected filter error")
	}
}
