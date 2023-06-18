package driver

import "fmt"

type OtherObjectType string

const (
	CanalType    OtherObjectType = "canal"
	BifrostType  OtherObjectType = "bifrost"
	TableMapType OtherObjectType = "tableMap"
)

type OtherOutputType struct {
	Name  string `json:"name"`
	Value string `json:"value"`
}

var otherOutputTypesList []OtherOutputType

func init() {
	otherOutputTypesList = make([]OtherOutputType, 3)
	otherOutputTypesList[0] = OtherOutputType{
		Name:  string(BifrostType),
		Value: "",
	}
	otherOutputTypesList[1] = OtherOutputType{
		Name:  string(CanalType),
		Value: string(CanalType),
	}
	otherOutputTypesList[2] = OtherOutputType{
		Name:  string(TableMapType),
		Value: string(TableMapType),
	}

}

func GetSupportedOtherOutputTypeList() []OtherOutputType {
	return otherOutputTypesList
}

func ToOtherObject(data *PluginDataType, otherObjectType OtherObjectType) (interface{}, error) {
	switch otherObjectType {
	case BifrostType:
		return data, nil
	case CanalType:
		return data.ToCanalJsonObject()
	case TableMapType:
		return data.ToTableMapObject()
	}
	return data, fmt.Errorf("not supported %s", otherObjectType)
}
