package src

import (
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

func TestTransferToTypeByColumnType_Starrocks(t *testing.T) {
	type ColumnTypeStruct struct {
		columnTypeList []string
		nullable       bool
		destColumnType string
	}
	var caseList = []ColumnTypeStruct{
		{
			columnTypeList: []string{"uint64"},
			nullable:       false,
			destColumnType: "VARCHAR(20)",
		},
		{
			columnTypeList: []string{"int64", "uint32"},
			nullable:       false,
			destColumnType: "BIGINT(20)",
		},
		{
			columnTypeList: []string{"int32", "uint24", "int24", "uint16"},
			nullable:       false,
			destColumnType: "INT(11)",
		},
		{
			columnTypeList: []string{"int16", "year(4)", "year(2)", "year", "uint8"},
			nullable:       false,
			destColumnType: "SMALLINT(6)",
		},
		{
			columnTypeList: []string{"int8", "bool"},
			nullable:       false,
			destColumnType: "TINYINT(4)",
		},
		{
			columnTypeList: []string{"float"},
			nullable:       false,
			destColumnType: "FLOAT",
		},
		{
			columnTypeList: []string{"double", "real"},
			nullable:       false,
			destColumnType: "DOUBLE",
		},
		{
			columnTypeList: []string{"decimal", "numeric"},
			nullable:       false,
			destColumnType: "DECIMAL",
		},
		{
			columnTypeList: []string{"decimal(9,2)"},
			nullable:       false,
			destColumnType: "Decimal(9,2)",
		},
		{
			columnTypeList: []string{"decimal(19,5)"},
			nullable:       false,
			destColumnType: "Decimal(19,5)",
		},
		{
			columnTypeList: []string{"decimal(38,2)"},
			nullable:       false,
			destColumnType: "Decimal(38,2)",
		},
		{
			columnTypeList: []string{"decimal(39,2)"},
			nullable:       false,
			destColumnType: "VARCHAR(78)",
		},
		{
			columnTypeList: []string{"decimal(88,2)"},
			nullable:       false,
			destColumnType: "VARCHAR(255)",
		},
		{
			columnTypeList: []string{"date", "Nullable(date)"},
			nullable:       false,
			destColumnType: "DATE",
		},
		{
			columnTypeList: []string{"json"},
			nullable:       false,
			destColumnType: "JSON",
		},
		{
			columnTypeList: []string{"time"},
			nullable:       false,
			destColumnType: "VARCHAR(10)",
		},
		{
			columnTypeList: []string{"enum"},
			nullable:       false,
			destColumnType: "VARCHAR(765)",
		},
		{
			columnTypeList: []string{"set"},
			nullable:       false,
			destColumnType: "VARCHAR(2048)",
		},
		{
			columnTypeList: []string{"string", "longblob", "longtext"},
			nullable:       false,
			destColumnType: "VARCHAR(163841)",
		},
		{
			columnTypeList: []string{"double(9,2)", "real(10,2)"},
			nullable:       false,
			destColumnType: "DOUBLE",
		},
		{
			columnTypeList: []string{"float(9,2)"},
			nullable:       false,
			destColumnType: "FLOAT",
		},
		{
			columnTypeList: []string{"bit"},
			nullable:       false,
			destColumnType: "BIGINT(20)",
		},
		{
			columnTypeList: []string{"timestamp(6)", "datetime(3)"},
			nullable:       false,
			destColumnType: "DATETIME",
		},
		{
			columnTypeList: []string{"time(6)", "time(1)"},
			nullable:       false,
			destColumnType: "VARCHAR(16)",
		},
		{
			columnTypeList: []string{"enum('a','b')"},
			nullable:       false,
			destColumnType: "VARCHAR(765)",
		},
		{
			columnTypeList: []string{"set('a','b')"},
			nullable:       false,
			destColumnType: "VARCHAR(2048)",
		},
		{
			columnTypeList: []string{"char(1)"},
			nullable:       false,
			destColumnType: "CHAR(1)",
		},
		{
			columnTypeList: []string{"char(255)"},
			nullable:       false,
			destColumnType: "CHAR(255)",
		},
		{
			columnTypeList: []string{"varchar(500)"},
			nullable:       false,
			destColumnType: "VARCHAR(1500)",
		},

		{
			columnTypeList: []string{"Nullable(varchar(500))"},
			nullable:       false,
			destColumnType: "VARCHAR(1500)",
		},
		{
			columnTypeList: []string{"Nullable(int64)"},
			nullable:       true,
			destColumnType: "BIGINT(20) DEFAULT NULL",
		},
	}

	c := &Conn{}
	for _, caseInfo := range caseList {
		for _, columnType := range caseInfo.columnTypeList {
			Convey(caseInfo.destColumnType, t, func() {
				toDestColumnType := c.TransferToTypeByColumnType_Starrocks(columnType, caseInfo.nullable)
				So(toDestColumnType, ShouldEqual, caseInfo.destColumnType)
			})
		}
	}
}
