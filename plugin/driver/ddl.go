package driver

import "strings"

func (c *PluginDataType) IsDDL() (isDDL bool) {
	if len(c.Query) < 4 {
		return
	}
	switch strings.ToUpper(c.Query[0:4]) {
	//drop,create alter,rename,truncate
	case "DROP", "CREA", "ALTE", "RENA", "TRUN":
		isDDL = true
		return
	}
	return
}
