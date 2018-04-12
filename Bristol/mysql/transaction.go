package mysql

type mysqlTx struct {
	mc *mysqlConn
}

func (tx *mysqlTx) Commit() (e error) {
	e = tx.mc.exec("COMMIT")
	tx.mc = nil
	return
}

func (tx *mysqlTx) Rollback() (e error) {
	e = tx.mc.exec("ROLLBACK")
	tx.mc = nil
	return
}
