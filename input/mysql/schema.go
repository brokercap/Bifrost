package mysql

import (
	"database/sql/driver"
	"fmt"
	"github.com/brokercap/Bifrost/Bristol/mysql"
	inputDriver "github.com/brokercap/Bifrost/input/driver"
	"log"
	"strings"
	"time"
)

func (c *MysqlInput) GetConn() mysql.MysqlConnection {
	db := mysql.NewConnect(c.inputInfo.ConnectUri)
	return db
}

func (c *MysqlInput) GetSchemaList() ([]string, error) {
	defer func() {
		if err := recover(); err != nil {

		}
	}()
	db := c.GetConn()
	if db != nil {
		defer db.Close()
	}
	databaseList := make([]string, 0)
	sql := "select `SCHEMA_NAME` from `information_schema`.`SCHEMATA`"
	p := make([]driver.Value, 0)
	rows, err := db.Query(sql, p)
	defer rows.Close()
	if err != nil {
		log.Printf("%v\n", err)
		return databaseList, nil
	}

	for {
		dest := make([]driver.Value, 1, 1)
		err := rows.Next(dest)
		if err != nil {
			break
		}
		var DatabaseName string
		DatabaseName = dest[0].(string)
		databaseList = append(databaseList, DatabaseName)
	}
	return databaseList, nil
}

func (c *MysqlInput) GetSchemaTableList(schema string) (tableList []inputDriver.TableList, err error) {
	defer func() {
		if err := recover(); err != nil {

		}
	}()
	db := c.GetConn()
	if db != nil {
		defer db.Close()
	}
	tableList = make([]inputDriver.TableList, 0)
	sql := "SELECT TABLE_NAME,TABLE_TYPE FROM `information_schema`.`TABLES` WHERE TABLE_SCHEMA = ?"
	p := make([]driver.Value, 0)
	p = append(p, schema)
	rows, err := db.Query(sql, p)
	defer rows.Close()
	if err != nil {
		log.Printf("%v\n", err)
		return
	}

	for {
		dest := make([]driver.Value, 2, 2)
		err := rows.Next(dest)
		if err != nil {
			break
		}
		var tableName string
		var tableType string
		tableName = dest[0].(string)
		tableType = dest[1].(string)
		tableList = append(tableList, inputDriver.TableList{TableName: tableName, TableType: tableType})
	}
	return
}

func (c *MysqlInput) GetSchemaTableFieldList(schema string, table string) (FieldList []inputDriver.TableFieldInfo, err error) {
	return c.GetSchemaTableFieldList0(schema, table)
}

func (c *MysqlInput) GetSchemaTableFieldList0(schema string, table string) (FieldList []inputDriver.TableFieldInfo, err error) {
	defer func() {
		if err := recover(); err != nil {

		}
	}()
	db := c.GetConn()
	if db != nil {
		defer db.Close()
	}
	FieldList = make([]inputDriver.TableFieldInfo, 0)
	sql := "SELECT `COLUMN_NAME`,`COLUMN_DEFAULT`,`IS_NULLABLE`,`COLUMN_TYPE`,`COLUMN_KEY`,`EXTRA`,`COLUMN_COMMENT`,`DATA_TYPE`,`NUMERIC_PRECISION`,`NUMERIC_SCALE` FROM `information_schema`.`columns` WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? "
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
		dest := make([]driver.Value, 10, 10)
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
		COLUMN_KEY = dest[4].(string)
		EXTRA = dest[5].(string)
		COLUMN_COMMENT = dest[6].(string)
		DATA_TYPE = dest[7].(string)

		if dest[8] == nil {
			NUMERIC_PRECISION = nil
		} else {
			switch dest[8].(type) {
			case uint32:
				t := uint64(dest[8].(uint32))
				NUMERIC_PRECISION = &t
			case uint64:
				t := dest[8].(uint64)
				NUMERIC_PRECISION = &t
			}
		}
		if dest[9] == nil {
			NUMERIC_SCALE = nil
		} else {
			switch dest[9].(type) {
			case uint32:
				t := uint64(dest[9].(uint32))
				NUMERIC_PRECISION = &t
			case uint64:
				t := dest[9].(uint64)
				NUMERIC_PRECISION = &t
			}
		}

		var IsNullable bool
		switch strings.ToUpper(IS_NULLABLE) {
		case "YES", "TRUE":
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

func (c *MysqlInput) CheckPrivilege() (err error) {
	defer func() {
		if err0 := recover(); err0 != nil {
			err = fmt.Errorf("%s", err0)
		}
	}()
	db := c.GetConn()
	if db != nil {
		db.Close()
	}
	err = CheckUserSlavePrivilege(db)
	return
}

func (c *MysqlInput) CheckUri(CheckPrivilege bool) (CheckUriResult inputDriver.CheckUriResult, err error) {
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
	if CheckPrivilege {
		err = CheckUserSlavePrivilege(dbconn)
		if err != nil {
			return
		}
	}
	Msg := make([]string, 0)
	MasterBinlogInfo := GetBinLogInfo(dbconn)
	if MasterBinlogInfo.File != "" {
		CheckUriResult.BinlogFile = MasterBinlogInfo.File
		CheckUriResult.BinlogPosition = MasterBinlogInfo.Position
		CheckUriResult.Gtid = MasterBinlogInfo.Executed_Gtid_Set
		CheckUriResult.ServerId = GetServerId(dbconn)
		variablesMap := GetVariables(dbconn, "binlog_format")
		BinlogRowImageMap := GetVariables(dbconn, "binlog_row_image")
		if _, ok := variablesMap["binlog_format"]; ok {
			binlogFormat := variablesMap["binlog_format"]
			switch strings.ToLower(binlogFormat) {
			case "row":
				break
			default:
				Msg = append(Msg, fmt.Sprintf("binlog_format(%s) != row", binlogFormat))
			}
			CheckUriResult.BinlogFormat = binlogFormat
		}
		if binlogRowImage, ok := BinlogRowImageMap["binlog_row_image"]; ok {
			switch strings.ToLower(binlogRowImage) {
			case "full":
				break
			default:
				Msg = append(Msg, fmt.Sprintf("binlog_row_image(%s) != full", binlogRowImage))
			}
			CheckUriResult.BinlogRowImage = binlogRowImage
		}
	} else {
		err = fmt.Errorf("The binlog maybe not open,or no replication client privilege(s).you can show log more.")
	}
	MasterVersion := GetMySQLVersion(dbconn)
	if strings.Contains(MasterVersion, "MariaDB") {
		m := GetVariables(dbconn, "gtid_binlog_pos")
		if gtidBinlogPos, ok := m["gtid_binlog_pos"]; ok {
			CheckUriResult.Gtid = gtidBinlogPos
		}
	}
	return
}

func (c *MysqlInput) GetCurrentPosition() (p *inputDriver.PluginPosition, err error) {
	defer func() {
		if err0 := recover(); err0 != nil {
			err = fmt.Errorf("%s", err0)
		}
	}()
	dbconn := c.GetConn()
	if dbconn == nil {
		err = fmt.Errorf("db conn ,uknow error;请排查 Bifrost 机器 到 MySQL 机器网络是否正常，防火墙是否开放等！")
		return
	}
	defer dbconn.Close()
	MasterBinlogInfo := GetBinLogInfo(dbconn)
	MasterVersion := GetMySQLVersion(dbconn)
	if strings.Contains(MasterVersion, "MariaDB") {
		m := GetVariables(dbconn, "gtid_binlog_pos")
		if gtidBinlogPos, ok := m["gtid_binlog_pos"]; ok {
			MasterBinlogInfo.Executed_Gtid_Set = gtidBinlogPos
		}
	}

	p = &inputDriver.PluginPosition{
		GTID:           MasterBinlogInfo.Executed_Gtid_Set,
		BinlogFileName: MasterBinlogInfo.File,
		BinlogPostion:  uint32(MasterBinlogInfo.Position),
		Timestamp:      uint32(time.Now().Unix()),
		EventID:        0,
	}
	return
}

func (c *MysqlInput) GetVersion() (Version string, err error) {
	defer func() {
		if err0 := recover(); err0 != nil {
			err = fmt.Errorf("%s", err0)
		}
	}()
	db := c.GetConn()
	if db == nil {
		err = fmt.Errorf("db conn ,uknow error;请排查 Bifrost 机器 到 MySQL 机器网络是否正常，防火墙是否开放等！")
	} else {
		defer db.Close()
	}
	Version = GetMySQLVersion(db)
	return
}
