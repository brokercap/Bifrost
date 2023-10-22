package src

import (
	dbDriver "database/sql/driver"
	"fmt"
	pluginDriver "github.com/brokercap/Bifrost/plugin/driver"
	"log"
	"runtime/debug"
)

func (This *Conn) IsStarRocks() bool {
	return This.isStarRocks
}

func (This *Conn) GetStarRocksBeCount() int {
	return This.starRocksBeCount
}

func (This *Conn) initIsStarrock() {
	defer func() {
		if err := recover(); err != nil {
			log.Printf("[ERROR] output[%s] initIsStarrock recover:%+v \n", OutputName, string(debug.Stack()))
			return
		}
	}()
	if This.conn == nil {
		return
	}
	backendsList, _ := This.conn.ShowBackends()
	if len(backendsList) == 0 {
		return
	}
	// starrocks show backends 列表中,存在 BePort 这个字段,代表 Be 节点的端口
	if _, ok := backendsList[0]["BePort"]; !ok {
		return
	}
	This.isStarRocks = true
	This.starRocksBeCount = len(backendsList)
}

func (This *Conn) StarRocksDelete(SchemaName, TableName string, pks []string, pksWhereList [][]string) error {
	if len(pks) == 0 || len(pksWhereList) == 0 {
		return nil
	}
	sql := fmt.Sprintf("DELETE FROM `%s`.`%s` WHERE ", SchemaName, TableName)
	whereArgsList := make([][]string, len(pks))
	for i, pkName := range pks {
		if i == 0 {
			sql += fmt.Sprintf("%s IN (?)", pkName)
		} else {
			sql += fmt.Sprintf("AND %s IN (?)", pkName)
		}
		whereArgsList[i] = make([]string, 0)
	}

	for _, pksValueList := range pksWhereList {
		if len(pksValueList) != len(pks) {
			log.Printf("[ERROR] output[%s] StarRocksDelete pks len != %d, pks fileds:%+v, but current val:%+v \n", OutputName, len(pks), pks, pksValueList)
			continue
		}
		for i, pksVal := range pksValueList {
			whereArgsList[i] = append(whereArgsList[i], pksVal)
		}
	}
	sqlArgs := make([]dbDriver.Value, len(whereArgsList))
	for i := range whereArgsList {
		//sqlArgs[i] = strings.Replace(strings.Trim(fmt.Sprint(whereArgs), "[]"), " ", "','", -1)
		sqlArgs[i] = whereArgsList[i]
	}
	_, err := This.conn.conn.Exec(sql, sqlArgs)
	return err
}

func (This *Conn) StarRocksInsert(list []*pluginDriver.PluginDataType) (errData *pluginDriver.PluginDataType, err error) {
	if len(list) == 0 {
		return nil, nil
	}
	var SchemaNme = This.GetSchemaName(list[0])
	var TableName = This.GetTableName(list[0])
	var valList = make([]dbDriver.Value, 0)
	fields := ""
	values := ""
	for _, v := range This.p.Field {
		if fields == "" {
			fields = "`" + v.ToField + "`"
			values = "?"
		} else {
			fields += ",`" + v.ToField + "`"
			values += ",?"
		}
	}
	sql := fmt.Sprintf("INSERT INTO `%s`.`%s` (%s) VALUES ", SchemaNme, TableName, fields)
	//将update, delete,insert 的数据全转成  insert 语句
	var k int
	var isFirst = true
LOOP:
	for i := 0; i < len(list); i++ {
		data := list[i]
		switch data.EventType {
		case "update":
			k = 1
			break
		case "insert", "delete":
			k = 0
			break
		default:
			continue
		}

		// 这里给每行数据,定义一个 list,防止是中途某一行数据是被允许跳过的,不需要同步的那种情况
		tmlValList := make([]dbDriver.Value, len(This.p.Field))
		for j, v := range This.p.Field {
			var toV dbDriver.Value
			fromVal := This.getMySQLData(data, k, v.FromMysqlField)
			toV, err = This.dataTypeTransfer(fromVal, v.ToField, v.ToFieldType, v.ToFieldDefault)
			if err != nil {
				log.Printf("[ERROR] output[%s] dataTypeTransfer from field:%s value:%+v to field:%s(%s) \n", OutputName, v.FromMysqlField, fromVal, v.ToField, v.ToFieldType)
				if !This.p.BifrostMustBeSuccess {
					err = nil
					log.Printf("[WARN] output[%s] auto skip data:%+v \n", OutputName, data)
					continue LOOP
				}
				if This.CheckDataSkip(data) {
					log.Printf("[WARN] output[%s] use skip data:%+v \n", OutputName, data)
					This.err = nil
					continue LOOP
				}
				return data, This.err
			}
			tmlValList[j] = toV
		}
		// sql 拼接放到最后是因为上面的所有逻辑中是有中途跳过
		valList = append(valList, tmlValList...)
		if isFirst {
			sql += fmt.Sprintf("(%s)", values)
			isFirst = false
		} else {
			sql += fmt.Sprintf(",(%s)", values)
		}
	}
	// 假如数据转换处理后,没有一条数据需要被同步,则直接跳过
	if len(valList) == 0 {
		return nil, nil
	}
	_, err = This.conn.conn.Exec(sql, valList)
	if err != nil {
		return list[0], err
	}
	return
}
