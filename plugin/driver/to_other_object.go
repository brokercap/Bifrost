package driver

import "fmt"

type OtherObjectType string

const (
	CanalType   OtherObjectType = "canal"
	BifrostType OtherObjectType = "bifrost"
)

func ToOtherObject(data *PluginDataType, otherObjectType OtherObjectType) (interface{}, error) {
	switch otherObjectType {
	case CanalType:
		return data.ToCanalJsonObject()
	}
	return data, fmt.Errorf("not supported %s", otherObjectType)
}
