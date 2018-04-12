package mysql

type mysqlResult struct {
	affectedRows int64
	insertId     int64
}

func (res mysqlResult) LastInsertId() (int64, error) {
	return res.insertId, nil
}

func (res mysqlResult) RowsAffected() (int64, error) {
	return res.affectedRows, nil
}
