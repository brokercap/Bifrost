package mysql

import "testing"

func TestGtid_Parse(t *testing.T) {
	gtid := NewMySQLGtid("04038bcc-fd0c-11e7-9cc5-000c29db6599:1-17")
	err := gtid.Parse()
	if err != nil {
		t.Fatal(err)
	}

}

func TestGtid_ParseInterval(t *testing.T) {
	gtid := NewMySQLGtid("04038bcc-fd0c-11e7-9cc5-000c29db6599:1-17")
	var m *Intervals
	var err error
	m, err = gtid.ParseInterval(":1-17")
	if err != nil {
		t.Log(err)
	}
	m, err = gtid.ParseInterval(":18")
	if err != nil {
		t.Log(err)
	}
	t.Log(m)
}
