// documentation:
// https://dev.mysql.com/doc/internals/en/com-binlog-dump-gtid.html
package mysql

import (
	"bytes"
	"encoding/binary"
	"fmt"
	uuid "github.com/satori/go.uuid"
	"io"
	"regexp"
	"strconv"
	"strings"
	"sync"
)

func NewMySQLGtid(GtidSet string) *MySQLGtid {
	gtid := &MySQLGtid{Gtid: GtidSet}
	return gtid
}

type Intervals struct {
	Start int64
	Stop  int64
}

type MySQLGtid struct {
	Gtid      string
	sid       uuid.UUID
	intervals []*Intervals
}

func (This *MySQLGtid) Parse() (err error) {
	var RegularxEpression = `^([0-9a-fA-F]{8}(?:-[0-9a-fA-F]{4}){3}-[0-9a-fA-F]{12})((?::[0-9-]+)+)$`
	reqTagAll, _ := regexp.Compile(RegularxEpression)
	m := reqTagAll.FindAllStringSubmatch(This.Gtid, -1)
	if len(m) == 0 || len(m[0]) == 0 {
		return fmt.Errorf("GTID format is incorrect")
	}
	This.sid, err = uuid.FromString(m[0][1])
	if err != nil {
		return err
	}
	for _, v := range strings.Split(m[0][2], ":") {
		if v == "" {
			continue
		}
		intervals, err := This.ParseInterval(v)
		if err != nil {
			return err
		}
		This.intervals = append(This.intervals, intervals)
	}
	return
}

func (This *MySQLGtid) ParseInterval(gtidIntervalsStr string) (*Intervals, error) {
	m := strings.Split(gtidIntervalsStr, "-")
	if len(m) == 0 {
		return nil, fmt.Errorf("GTID format is incorrect")
	}
	intervals := &Intervals{}
	switch len(m) {
	case 1:
		intA, err := strconv.ParseInt(m[0], 10, 64)
		if err != nil {
			return nil, fmt.Errorf("GTID format is incorrect")
		}
		intervals.Start = intA
		intervals.Stop = intA + 1
		break
	case 2:
		intA, err := strconv.ParseInt(m[0], 10, 64)
		if err != nil {
			return nil, fmt.Errorf("GTID format is incorrect")
		}
		intervals.Start = intA
		intA, err = strconv.ParseInt(m[1], 10, 64)
		if err != nil {
			return nil, fmt.Errorf("GTID format is incorrect")
		}
		intervals.Stop = intA + 1
	}
	return intervals, nil
}

func (This *MySQLGtid) Encode(w io.Writer) {
	w.Write(This.sid.Bytes())
	binary.Write(w, binary.LittleEndian, int64(len(This.intervals)))
	for _, v := range This.intervals {
		binary.Write(w, binary.LittleEndian, v.Start)
		binary.Write(w, binary.LittleEndian, v.Stop)
	}
}

func (This *MySQLGtid) String() string {
	return This.Gtid
	/*
		gtid := This.sid.String()
		for _, v := range This.intervals {
			gtid += fmt.Sprint(":%d-%d",v.Start,v.Stop)
		}
		return gtid
	*/
}

func NewMySQLGtidSet(GtidStr string) *MySQLGtidSet {
	gtidSet := &MySQLGtidSet{gtids: make(map[string]*MySQLGtid, 0), GtidStr: GtidStr}
	return gtidSet
}

type MySQLGtidSet struct {
	sync.RWMutex
	gtids   map[string]*MySQLGtid
	GtidStr string
}

func (This *MySQLGtidSet) Init() (err error) {
	This.Lock()
	defer This.Unlock()
	for _, gtidStr := range strings.Split(This.GtidStr, ",") {
		gtidInfo := NewMySQLGtid(gtidStr)
		err = gtidInfo.Parse()
		if err != nil {
			return err
		}
		This.gtids[gtidInfo.sid.String()] = gtidInfo
	}
	return nil
}

func (This *MySQLGtidSet) ReInit() (err error) {
	This.GtidStr = This.String()
	return This.Init()
}

func (This *MySQLGtidSet) Encode() []byte {
	var buf bytes.Buffer
	This.RLock()
	binary.Write(&buf, binary.LittleEndian, uint64(len(This.gtids)))
	for _, gtid := range This.gtids {
		gtid.Encode(&buf)
	}
	This.RUnlock()
	return buf.Bytes()
}

func (This *MySQLGtidSet) String() string {
	gtidStr := ""
	This.RLock()
	for _, gtidInfo := range This.gtids {
		if gtidStr == "" {
			gtidStr = gtidInfo.String()
		} else {
			gtidStr += "," + gtidInfo.String()
		}
	}
	This.RUnlock()
	return gtidStr
}

func (This *MySQLGtidSet) Update(gtid string) error {
	gtidInfo := NewMySQLGtid(gtid)
	err := gtidInfo.Parse()
	if err != nil {
		return err
	}
	This.Lock()
	This.gtids[gtidInfo.sid.String()] = gtidInfo
	This.Unlock()
	return nil
}
