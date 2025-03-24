package mysql

import (
	"database/sql/driver"
	"errors"
	"fmt"
	inputDriver "github.com/brokercap/Bifrost/input/driver"
	"log"
	"strings"
)

func init() {
	inputDriver.Register("mysql(other)", NewMySqlOtherInputPlugin, VERSION, BIFROST_VERSION)
}

type MysqlOtherInput struct {
	MysqlInput
}

func NewMySqlOtherInputPlugin() inputDriver.Driver {
	return &MysqlOtherInput{}
}

func (c *MysqlOtherInput) GetUriExample() (string, string) {
	notesHtml := `
		<p><span class="help-block m-b-none">Only Supported Batch</span></p>
	`
	return "root:root@tcp(127.0.0.1:3306)/test", notesHtml
}

func (c *MysqlOtherInput) IsSupported(supportType inputDriver.SupportType) bool {
	switch supportType {
	case inputDriver.SupportFull:
		return true
	default:
		return false
	}
}

func (c *MysqlOtherInput) Start(ch chan *inputDriver.PluginStatus) error {
	return fmt.Errorf("not support inrc data!")
}

func (c *MysqlOtherInput) CheckPrivilege() (err error) {
	defer func() {
		if err0 := recover(); err0 != nil {
			err = fmt.Errorf("%s", err0)
		}
	}()
	db := c.GetConn()
	if db == nil {
		err = fmt.Errorf("db conn ,uknow error;请排查 Bifrost 机器 到 MySQL 机器网络是否正常，防火墙是否开放等！")
	} else {
		db.Close()
	}
	return
}

func (c *MysqlOtherInput) CheckUri(CheckPrivilege bool) (CheckUriResult inputDriver.CheckUriResult, err error) {
	defer func() {
		if err0 := recover(); err0 != nil {
			err = fmt.Errorf("%s", err0)
		}
	}()
	dbconn := c.GetConn()
	if dbconn == nil {
		err = fmt.Errorf("db conn ,uknow error;请排查 Bifrost 机器 到 MySQL 机器网络是否正常，防火墙是否开放等！")
	}
	if err != nil {
		return
	}
	defer dbconn.Close()
	CheckUriResult = inputDriver.CheckUriResult{
		BinlogFile:     "bifrost.000001",
		BinlogPosition: 4,
		BinlogFormat:   "row",
		BinlogRowImage: "full",
		Gtid:           "",
		ServerId:       1,
	}
	return
}

func (c *MysqlOtherInput) GetSchemaTableFieldList(schema string, table string) (FieldList []inputDriver.TableFieldInfo, err error) {
	var isCK bool
	isCK, err = c.IsClickHouse()
	if err != nil {
		return
	}
	if isCK {
		return c.GetClickHouseSchemaTableFieldList(schema, table)
	} else {
		return c.GetSchemaTableFieldList0(schema, table)
	}
}

func (c *MysqlOtherInput) IsClickHouse() (ok bool, err error) {
	defer func() {
		if err0 := recover(); err0 != nil {
			err = errors.New(fmt.Sprint(err0))
		}
	}()
	sysTableFieldList, err0 := c.GetClickHouseSchemaTableFieldList("system", "tables")
	if err0 != nil {
		return false, err0
	}
	if len(sysTableFieldList) > 0 {
		return true, nil
	}
	return false, nil
}

func (c *MysqlInput) GetClickHouseSchemaTableFieldList(schema string, table string) (FieldList []inputDriver.TableFieldInfo, err error) {
	defer func() {
		if err := recover(); err != nil {

		}
	}()
	db := c.GetConn()
	if db != nil {
		defer db.Close()
	}
	FieldList = make([]inputDriver.TableFieldInfo, 0)
	sql := "SELECT `COLUMN_NAME`,`COLUMN_DEFAULT`,`IS_NULLABLE`,`COLUMN_TYPE`,`COLUMN_COMMENT`,`DATA_TYPE`,`NUMERIC_PRECISION`,`NUMERIC_SCALE` FROM `information_schema`.`columns` WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? "
	p := make([]driver.Value, 0)
	p = append(p, schema)
	p = append(p, table)
	rows, err := db.Query(sql, p)
	defer rows.Close()
	if err != nil {
		log.Printf("%v\n", err)
		return
	}

	for {
		dest := make([]driver.Value, 8, 8)
		err := rows.Next(dest)
		if err != nil {
			break
		}
		var COLUMN_NAME string
		var COLUMN_DEFAULT *string
		var IS_NULLABLE string
		var COLUMN_TYPE string
		var EXTRA string
		var COLUMN_COMMENT string
		var DATA_TYPE string
		var NUMERIC_PRECISION *uint64
		var NUMERIC_SCALE *uint64
		var COLUMN_KEY string

		COLUMN_NAME = dest[0].(string)
		if dest[1] == nil {
			COLUMN_DEFAULT = nil
		} else {
			t := dest[1].(string)
			COLUMN_DEFAULT = &t
		}

		IS_NULLABLE = dest[2].(string)
		COLUMN_TYPE = dest[3].(string)
		COLUMN_COMMENT = dest[4].(string)
		DATA_TYPE = dest[5].(string)

		if dest[6] == nil {
			NUMERIC_PRECISION = nil
		} else {
			switch dest[6].(type) {
			case uint32:
				t := uint64(dest[6].(uint32))
				NUMERIC_PRECISION = &t
			case uint64:
				t := dest[6].(uint64)
				NUMERIC_PRECISION = &t
			}
		}
		if dest[7] == nil {
			NUMERIC_SCALE = nil
		} else {
			switch dest[7].(type) {
			case uint32:
				t := uint64(dest[7].(uint32))
				NUMERIC_PRECISION = &t
			case uint64:
				t := dest[7].(uint64)
				NUMERIC_PRECISION = &t
			}
		}

		var IsNullable bool
		switch strings.ToUpper(IS_NULLABLE) {
		case "YES", "TRUE", "1":
			IsNullable = true
		default:
			break
		}
		var IsAutoIncrement bool
		if strings.ToLower(EXTRA) == "auto_increment" {
			IsAutoIncrement = true
		}

		FieldList = append(FieldList, inputDriver.TableFieldInfo{
			ColumnName:       &COLUMN_NAME,
			ColumnDefault:    COLUMN_DEFAULT,
			IsNullable:       IsNullable,
			ColumnType:       &COLUMN_TYPE,
			IsAutoIncrement:  IsAutoIncrement,
			Comment:          &COLUMN_COMMENT,
			DataType:         &DATA_TYPE,
			NumericPrecision: NUMERIC_PRECISION,
			NumericScale:     NUMERIC_SCALE,
			ColumnKey:        &COLUMN_KEY,
		})
	}
	return
}
