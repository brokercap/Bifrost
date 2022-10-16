package driver

import "fmt"

type OtherObjectType string

const (
	CanalType OtherObjectType = "canal"
)

func ToOtherObject(data PluginDataType, otherObjectType OtherObjectType) (interface{}, error) {
	switch otherObjectType {
	case CanalType:
		return data.ToCanalJsonObject()
	}
	return nil, fmt.Errorf("not supported %s", otherObjectType)
}
