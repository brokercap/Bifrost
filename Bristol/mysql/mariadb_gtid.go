// documentation:
// mariadb gtid parser
package mysql

import (
	"bytes"
	"fmt"
	"io"
	"strconv"
	"strings"
	"sync"
)

func NewMariaDBGtid(GtidSet string) *MariaDBGtid {
	gtid := &MariaDBGtid{Gtid: GtidSet}
	return gtid
}

type MariaDBGtid struct {
	Gtid     string
	domainId uint32
	serverId uint32
	sequence uint64
}

func (This *MariaDBGtid) SetGtid(gtid string) {
	This.Gtid = gtid
}

func (This *MariaDBGtid) Parse() (err error) {
	t := strings.Split(This.Gtid, "-")
	if len(t) != 3 {
		return fmt.Errorf("invalid GTID: %s,MariaDB GTID must like DomainId-ServerId-Sequence", This.Gtid)
	}
	domainId, err := strconv.ParseUint(t[0], 10, 32)
	if err != nil {
		return fmt.Errorf("invalid GTID: %s,DomainId is not uint32", This.Gtid)
	}
	serverId, err := strconv.ParseUint(t[1], 10, 32)
	if err != nil {
		return fmt.Errorf("invalid GTID: %s,ServerId is not uint32", This.Gtid)
	}

	sequence, err := strconv.ParseUint(t[2], 10, 64)
	if err != nil {
		return fmt.Errorf("invalid GTID: %s,Sequence is not uint64", This.Gtid)
	}
	This.domainId = uint32(domainId)
	This.serverId = uint32(serverId)
	This.sequence = sequence
	return
}

func (This *MariaDBGtid) Encode(w io.Writer) {

}

func (This *MariaDBGtid) String() string {
	return fmt.Sprintf("%d-%d-%d", This.domainId, This.serverId, This.sequence)
}

func NewMariaDBGtidSet(GtidStr string) *MariaDBGtidSet {
	gtidSet := &MariaDBGtidSet{gtids: make(map[uint32]*MariaDBGtid, 0), GtidStr: GtidStr}
	return gtidSet
}

type MariaDBGtidSet struct {
	sync.RWMutex
	gtids   map[uint32]*MariaDBGtid
	GtidStr string
}

func (This *MariaDBGtidSet) Init() (err error) {
	This.Lock()
	defer This.Unlock()
	for _, gtidStr := range strings.Split(This.GtidStr, ",") {
		gtidInfo := NewMariaDBGtid(gtidStr)
		err = gtidInfo.Parse()
		if err != nil {
			return err
		}
		This.gtids[gtidInfo.domainId] = gtidInfo
	}
	return nil
}

func (This *MariaDBGtidSet) ReInit() (err error) {
	This.GtidStr = This.String()
	return This.Init()
}

func (This *MariaDBGtidSet) Encode() []byte {
	var buf bytes.Buffer
	sep := ""
	This.RLock()
	for _, gtid := range This.gtids {
		buf.WriteString(sep)
		buf.WriteString(gtid.String())
		sep = ","
	}
	This.RUnlock()
	return buf.Bytes()
}

func (This *MariaDBGtidSet) Update(gtid string) error {
	gtidInfo := NewMariaDBGtid(gtid)
	err := gtidInfo.Parse()
	if err != nil {
		return err
	}
	This.Lock()
	This.gtids[gtidInfo.domainId] = gtidInfo
	This.Unlock()
	return nil
}

func (This *MariaDBGtidSet) String() string {
	gtidStr := ""
	This.RLock()
	for _, gtidInfo := range This.gtids {
		if gtidStr == "" {
			gtidStr = gtidInfo.Gtid
		} else {
			gtidStr += "," + gtidInfo.Gtid
		}
	}
	This.RUnlock()
	return gtidStr
}
