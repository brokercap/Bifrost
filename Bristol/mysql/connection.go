package mysql

import (
	"bufio"
	"crypto/tls"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"
)

type mysqlConn struct {
	cfg            *config
	server         *serverSettings
	netConn        net.Conn
	bufReader      *bufio.Reader
	protocol       uint8
	sequence       uint8
	affectedRows   uint64
	insertId       uint64
	lastCmdTime    time.Time
	keepaliveTimer *time.Timer
	status         uint16
}

type config struct {
	user           string
	passwd         string
	net            string
	addr           string
	dbname         string
	params         map[string]string
	authPluginName string
	tlsConfig      *tls.Config
}

type serverSettings struct {
	protocol     byte
	version      string
	flags        ClientFlag
	charset      uint8
	scrambleBuff []byte
	threadID     uint32
	keepalive    int64
}

func (mc *mysqlConn) initConn(conn net.Conn) {
	mc.netConn = conn
	mc.bufReader = bufio.NewReader(mc.netConn)
}

// Handles parameters set in DSN
func (mc *mysqlConn) handleParams() (e error) {
	for param, val := range mc.cfg.params {
		switch param {
		// Charset
		case "charset":
			e = mc.exec("SET NAMES " + val)
			if e != nil {
				return
			}

		// TLS-Encryption
		case "tls":
			dbgLog.Print("TLS-Encryption not implemented yet")

		// Compression
		case "compress":
			dbgLog.Print("Compression not implemented yet")

		// We don't want to set keepalive as system var
		case "keepalive":
			continue

		// System Vars
		default:
			e = mc.exec("SET " + param + "=" + val + "")
			if e != nil {
				return
			}
		}
	}

	// KeepAlive
	if val, param := mc.cfg.params["keepalive"]; param {
		mc.server.keepalive, e = strconv.ParseInt(val, 10, 64)
		if e != nil {
			return errors.New("Invalid keepalive time")
		}

		// Get keepalive time by MySQL system var wait_timeout
		if mc.server.keepalive == 1 {
			val, e = mc.getSystemVar("wait_timeout")
			mc.server.keepalive, e = strconv.ParseInt(val, 10, 64)
			if e != nil {
				return errors.New("Error getting wait_timeout")
			}

			// Trigger 1min BEFORE wait_timeout
			if mc.server.keepalive > 60 {
				mc.server.keepalive -= 60
			}
		}

		if mc.server.keepalive > 0 {
			mc.lastCmdTime = time.Now()

			// Ping-Timer to avoid timeout
			mc.keepaliveTimer = time.AfterFunc(
				time.Duration(mc.server.keepalive)*time.Second, func() {
					var diff time.Duration
					for {
						// Fires only if diff > keepalive. Makes it collision safe
						for mc.netConn != nil &&
							mc.lastCmdTime.Unix()+mc.server.keepalive > time.Now().Unix() {
							diff = mc.lastCmdTime.Sub(time.Unix(time.Now().Unix()-mc.server.keepalive, 0))
							time.Sleep(diff)
						}
						if mc.netConn != nil {
							if e := mc.Ping(); e != nil {
								break
							}
						} else {
							return
						}
					}
				})
		}
	}
	return
}

func (mc *mysqlConn) Begin() (driver.Tx, error) {
	e := mc.exec("START TRANSACTION")
	if e != nil {
		return nil, e
	}

	return &mysqlTx{mc}, e
}

func (mc *mysqlConn) Close() (e error) {
	if mc.server.keepalive > 0 {
		mc.keepaliveTimer.Stop()
	}
	mc.writeCommandPacket(COM_QUIT)
	mc.bufReader = nil
	mc.netConn.Close()
	mc.netConn = nil
	return
}

func (mc *mysqlConn) Prepare(query string) (driver.Stmt, error) {
	// Send command
	e := mc.writeCommandPacket(COM_STMT_PREPARE, query)
	if e != nil {
		return nil, e
	}

	stmt := mysqlStmt{new(stmtContent)}
	stmt.mc = mc

	// Read Result
	var columnCount uint16
	columnCount, e = stmt.readPrepareResultPacket()

	if e != nil {
		return nil, e
	}
	if stmt.paramCount > 0 {
		stmt.params, e = stmt.mc.readColumns(stmt.paramCount)
		if e != nil {
			return nil, e
		}
	}

	if columnCount > 0 {
		_, e = stmt.mc.readUntilEOF()
		if e != nil {
			return nil, e
		}
	}

	return stmt, e
}

func (mc *mysqlConn) Exec(query string, args []driver.Value) (driver.Result, error) {
	if len(args) > 0 {
		var err error
		query, err = mc.interpolateParams(query, args)
		if err != nil {
			return nil, err
		}
	}
	mc.affectedRows = 0
	mc.insertId = 0

	e := mc.exec(query)
	if e != nil {
		return nil, e
	}

	if mc.affectedRows == 0 {
		return driver.ResultNoRows, e
	}

	return &mysqlResult{
			affectedRows: int64(mc.affectedRows),
			insertId:     int64(mc.insertId)},
		e
}

// Internal function to execute statements
func (mc *mysqlConn) exec(query string) (e error) {
	// Send command
	e = mc.writeCommandPacket(COM_QUERY, query)
	if e != nil {
		return
	}

	// Read Result
	resLen, e := mc.readResultSetHeaderPacket()
	if e != nil {
		return
	}

	mc.affectedRows = 0
	mc.insertId = 0

	if resLen > 0 {
		_, e = mc.readUntilEOF()
		if e != nil {
			return
		}

		mc.affectedRows, e = mc.readUntilEOF()
		if e != nil {
			return
		}
	}

	return
}

func (mc *mysqlConn) Query(query string, args []driver.Value) (driver.Rows, error) {
	if len(args) > 0 {
		var err error
		query, err = mc.interpolateParams(query, args)
		if err != nil {
			return nil, err
		}
	}
	return mc.query(query)
}

func (mc *mysqlConn) query(query string) (dataRows driver.Rows, e error) {
	e = mc.writeCommandPacket(COM_QUERY, query)
	if e != nil {
		return
	}

	// Read Result
	var resLen int
	resLen, e = mc.readResultSetHeaderPacket()
	if e != nil {
		return
	}
	if resLen == 0 {
		return nil, driver.ErrSkip
	}
	rows := mysqlRows{new(rowsContent)}
	rows.content.columns, e = mc.readColumns(resLen)
	if e != nil {
		return
	}
	e = mc.readStringRows(rows.content)
	return rows, e
}

func (mc *mysqlConn) interpolateParams(query string, args []driver.Value) (string, error) {
	if strings.Count(query, "?") != len(args) {
		return "", driver.ErrSkip
	}
	buf := make([]byte, 0)
	argPos := 0
	whereIndex := strings.Index(strings.ToUpper(query), "WHERE")

	for i := 0; i < len(query); i++ {
		q := strings.IndexByte(query[i:], '?')
		if q == -1 {
			buf = append(buf, query[i:]...)
			break
		}
		buf = append(buf, query[i:i+q]...)
		i += q

		arg := args[argPos]
		argPos++

		if arg == nil {
			buf = append(buf, "NULL"...)
			continue
		}

		switch v := arg.(type) {
		case int8, int16, int32, int:
			int64N, _ := strconv.ParseInt(fmt.Sprint(v), 10, 64)
			buf = strconv.AppendInt(buf, int64N, 10)
			break
		case uint8, uint16, uint32, uint:
			uint64N, _ := strconv.ParseUint(fmt.Sprint(v), 10, 64)
			buf = strconv.AppendUint(buf, uint64N, 10)
			break
		case int64:
			buf = strconv.AppendInt(buf, v, 10)
		case uint64:
			// Handle uint64 explicitly because our custom ConvertValue emits unsigned values
			buf = strconv.AppendUint(buf, v, 10)
		case float32:
			buf = strconv.AppendFloat(buf, float64(v), 'g', -1, 64)
		case float64:
			buf = strconv.AppendFloat(buf, v, 'g', -1, 64)
		case bool:
			if v {
				buf = append(buf, '1')
			} else {
				buf = append(buf, '0')
			}
		case time.Time:
			if v.IsZero() {
				buf = append(buf, "'0000-00-00'"...)
			} else {
				// 测试下来 在timestamp(0)的情况下,2006-01-02 15:04:05.999999 写,binlog 解析出来和写进去存在 1 秒 误差,暂不知道具体的原因
				// 建议使用string传进来,而不要使用time.Time类型
				timeStr := v.Format("2006-01-02 15:04:05.999999")
				buf = append(buf, '\'')
				if mc.isSupportedBackslash() {
					buf = escapeBytesBackslash(buf, []byte(timeStr))
				} else {
					buf = escapeBytesQuotes(buf, []byte(timeStr))
				}
				buf = append(buf, '\'')
			}
		case json.RawMessage:
			if v == nil {
				buf = append(buf, "NULL"...)
				continue
			}
			buf = append(buf, '\'')
			if mc.isSupportedBackslash() {
				buf = escapeBytesBackslash(buf, v)
			} else {
				buf = escapeBytesQuotes(buf, v)
			}
			buf = append(buf, '\'')
		case []byte:
			if v == nil {
				buf = append(buf, "NULL"...)
			} else {
				buf = appendArgsBufByBytes(buf, v, mc.isSupportedBackslash())
			}
		case string:
			buf = appendArgsBufByString(buf, v, mc.isSupportedBackslash())
			break
		case []string:
			if whereIndex > 0 && q > whereIndex {
				for ii, val := range v {
					if ii > 0 {
						buf = append(buf, ',')
					}
					buf = appendArgsBufByString(buf, val, mc.isSupportedBackslash())
				}
			} else {
				c, _ := json.Marshal(v)
				buf = appendArgsBufByBytes(buf, c, mc.isSupportedBackslash())
			}
			break
		case []int, []uint, []int8, []int16, []uint16, []int32, []uint32, []int64, []uint64:
			if whereIndex > 0 && q > whereIndex {
				whereInStr := strings.Replace(strings.Trim(fmt.Sprint(v), "[]"), " ", ",", -1)
				buf = appendArgsBufByString(buf, whereInStr, mc.isSupportedBackslash())
			} else {
				c, _ := json.Marshal(v)
				buf = appendArgsBufByBytes(buf, c, mc.isSupportedBackslash())
			}
			break
		default:
			return "", driver.ErrSkip
		}

		if len(buf)+4 > MAX_PACKET_SIZE {
			return "", driver.ErrSkip
		}
	}
	if argPos != len(args) {
		return "", driver.ErrSkip
	}
	return string(buf), nil
}

// Gets the value of the given MySQL System Variable
func (mc *mysqlConn) getSystemVar(name string) (val string, e error) {
	// Send command
	e = mc.writeCommandPacket(COM_QUERY, "SELECT @@"+name)
	if e != nil {
		return
	}

	// Read Result
	resLen, e := mc.readResultSetHeaderPacket()
	if e != nil {
		return
	}

	if resLen > 0 {
		var n uint64
		n, e = mc.readUntilEOF()
		if e != nil {
			return
		}

		var rows []*[][]byte
		rows, e = mc.readRows(int(n))
		if e != nil {
			return
		}

		val = string((*rows[0])[0])
	}

	return
}

// Executes a simple Ping-CMD to test or keepalive the connection
func (mc *mysqlConn) Ping() (e error) {
	// Send command
	e = mc.writeCommandPacket(COM_PING)
	if e != nil {
		return
	}

	// Read Result
	e = mc.readResultOK()
	return
}

func (mc *mysqlConn) markBadConn(err error) error {
	if mc == nil {
		return err
	}
	return driver.ErrBadConn
}

func (mc *mysqlConn) isSupportedBackslash() bool {
	if mc.status&STATUS_NO_BACK_SLASH_ESCAPES == 0 {
		return true
	}
	return false
}

func NewConnect(uri string) MysqlConnection {
	dbopen := &mysqlDriver{}
	conn, err := dbopen.Open(uri)
	if err != nil {
		panic(err)
	}
	return conn.(MysqlConnection)
}
