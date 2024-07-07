package mysql

import "strings"

type GTIDSet interface {
	Init() error

	ReInit() error
	String() string

	// Encode GTID set into binary format used in binlog dump commands
	Encode() []byte

	Update(GTID string) error
}

type DBType string

const (
	DB_TYPE_MYSQL   DBType = "mysql"
	DB_TYPE_MARIADB DBType = "mariadb"
)

func NewGTIDSet(gtids string) (gtidSetInfo GTIDSet, dbType DBType, err error) {
	seq := strings.Split(gtids, ",")[0]
	index := strings.Index(seq, ":")
	if index < 0 {
		if strings.Count(seq, "-") == 2 {
			dbType = DB_TYPE_MARIADB
			gtidSetInfo = NewMariaDBGtidSet(gtids)
		} else {
			dbType = DB_TYPE_MYSQL
			gtidSetInfo = NewMySQLGtidSet(gtids)
		}
	} else {
		dbType = DB_TYPE_MYSQL
		gtidSetInfo = NewMySQLGtidSet(gtids)
	}
	err = gtidSetInfo.Init()
	return
}

func CheckGtid(gtids string) (err error) {
	_, _, err = NewGTIDSet(gtids)
	return
}
