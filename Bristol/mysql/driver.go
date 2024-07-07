package mysql

import (
	"database/sql/driver"
	"errors"
	"net"
)

type mysqlDriver struct{}

func (d *mysqlDriver) Open(dsn string) (driver.Conn, error) {
	var e error
	// New mysqlConn
	mc := new(mysqlConn)
	mc.cfg = parseDSN(dsn)

	if mc.cfg.dbname == "" {
		e = errors.New("Incomplete or invalid DSN")
		return nil, e
	}

	// Connect to Server
	netConn, e := net.Dial(mc.cfg.net, mc.cfg.addr)
	if e != nil {
		return nil, e
	}
	mc.initConn(netConn)

	// Reading Handshake Initialization Packet
	e = mc.readInitPacket()
	if e != nil {
		return nil, e
	}

	// Send Client Authentication Packet
	e = mc.writeAuthPacket()
	if e != nil {
		return nil, e
	}

	// Read Result Packet
	e = mc.handleAuthResult()
	if e != nil {
		return nil, e
	}

	// Handle DSN Params
	e = mc.handleParams()
	if e != nil {
		return nil, e
	}

	return mc, e
}
