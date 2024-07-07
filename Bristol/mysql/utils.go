package mysql

import (
	"bytes"
	"crypto/sha1"
	"encoding/binary"
	"errors"
	"io"
	"log"
	"math"
	"os"
	"regexp"
	"strconv"
	"strings"
)

// Logger
var (
	errLog *log.Logger
	dbgLog *log.Logger
)

func init() {
	errLog = log.New(os.Stderr, "[MySQL] ", log.LstdFlags)
	dbgLog = log.New(os.Stdout, "[MySQL] ", log.LstdFlags)

	dsnPattern = regexp.MustCompile(
		`^(?:(?P<user>.*?)(?::(?P<passwd>.*))?@)?` + // [user[:password]@]
			`(?:(?P<net>[^\(]*)(?:\((?P<addr>[^\)]*)\))?)?` + // [net[(addr)]]
			`\/(?P<dbname>.*?)` + // /dbname
			`(?:\?(?P<params>[^\?]*))?$`) // [?param1=value1&paramN=valueN]
}

// Data Source Name Parser
var dsnPattern *regexp.Regexp

func parseDSN(dsn string) *config {
	cfg := new(config)
	cfg.params = make(map[string]string)

	matches := dsnPattern.FindStringSubmatch(dsn)
	names := dsnPattern.SubexpNames()

	var caCertFile, clientCertFile, clientKeyFile string
	var insecureSkipVerify = true
	var TLSServerName string

	for i, match := range matches {
		switch names[i] {
		case "user":
			cfg.user = match
		case "passwd":
			cfg.passwd = match
		case "net":
			cfg.net = match
		case "addr":
			cfg.addr = match
		case "dbname":
			cfg.dbname = match
		case "params":
			for _, v := range strings.Split(match, "&") {
				param := strings.SplitN(v, "=", 2)
				if len(param) != 2 {
					continue
				}
				switch param[0] {
				case "ca-cert":
					caCertFile = param[1]
				case "client-cert":
					clientCertFile = param[1]
				case "client-key":
					clientKeyFile = param[1]
				case "insecure-skip-verify":
					if strings.ToLower(param[1]) != "true" {
						insecureSkipVerify = false
					}
				case "servername":
					TLSServerName = param[1]
				default:
					cfg.params[param[0]] = param[1]
				}
			}
		}
	}

	// Set default network if empty
	if cfg.net == "" {
		cfg.net = "tcp"
	}

	// Set default adress if empty
	if cfg.addr == "" {
		cfg.addr = "127.0.0.1:3306"
	}

	if caCertFile != "" {
		var err error
		cfg.tlsConfig, err = NewClientTLSConfigWithFile(caCertFile, clientCertFile, clientKeyFile, insecureSkipVerify, TLSServerName)
		if err != nil {
			log.Println("[WARN] mysql tls config init error;", err.Error())
		}
	}

	return cfg
}

// Encrypt password using 4.1+ method
// http://forge.mysql.com/wiki/MySQL_Internals_ClientServer_Protocol#4.1_and_later
func scramblePassword(scramble, password []byte) (result []byte) {
	if len(password) == 0 {
		return
	}

	// stage1Hash = SHA1(password)
	crypt := sha1.New()
	crypt.Write(password)
	stage1Hash := crypt.Sum(nil)

	// scrambleHash = SHA1(scramble + SHA1(stage1Hash))
	// inner Hash
	crypt.Reset()
	crypt.Write(stage1Hash)
	scrambleHash := crypt.Sum(nil)

	// outer Hash
	crypt.Reset()
	crypt.Write(scramble)
	crypt.Write(scrambleHash)
	scrambleHash = crypt.Sum(nil)

	// token = scrambleHash XOR stage1Hash
	result = make([]byte, 20)
	for i := range result {
		result[i] = scrambleHash[i] ^ stage1Hash[i]
	}
	return
}

/******************************************************************************
*                       Read data-types from bytes                            *
******************************************************************************/

// Read a slice from the data slice
func readSlice(data []byte, delim byte) (slice []byte, e error) {
	pos := bytes.IndexByte(data, delim)
	if pos > -1 {
		slice = data[:pos]
	} else {
		slice = data
		e = io.EOF
	}
	return
}

func readLengthCodedBinary(data []byte) (b []byte, n int, isNull bool, e error) {
	// Get length
	num, n, e := bytesToLengthCodedBinary(data)
	if e != nil {
		return
	}

	// Check data length
	if len(data) < n+int(num) {
		e = io.EOF
		return
	}

	// Check if null
	if data[0] == 251 {
		isNull = true
	} else {
		isNull = false
	}

	// Get bytes
	b = data[n : n+int(num)]
	n += int(num)
	return
}

func readAndDropLengthCodedBinary(data []byte) (n int, e error) {
	// Get length
	num, n, e := bytesToLengthCodedBinary(data)
	if e != nil {
		return
	}

	// Check data length
	if len(data) < n+int(num) {
		e = io.EOF
		return
	}

	n += int(num)
	return
}

func readLengthEncodedInt(buf *bytes.Buffer) (num uint64, isNull bool, e error) {
	var b byte

	b, e = buf.ReadByte()
	if e != nil {
		return
	}

	switch {

	// 0-250: value of first byte
	case b <= 250:
		num = uint64(b)
		return

	// 251: NULL
	case b == 251:
		num = 0
		isNull = true
		return

	// 252: value of following 2
	case b == 252:
		var num16 uint16
		binary.Read(buf, binary.LittleEndian, &num16)
		num = uint64(num16)
		return

	// 253: value of following 3
	case b == 253:
		num, e = readFixedLengthInteger(buf, 3)
		return

	// 254: value of following 8
	case b == 254:
		e = binary.Read(buf, binary.LittleEndian, &num)
		return

	default:
		e = errors.New("undefined value (0xff) length encoded integer")
		return
	}

	return
}

/******************************************************************************
*                       Convert from and to bytes                             *
******************************************************************************/

func byteToUint8(b byte) (n uint8) {
	n |= uint8(b)
	return
}

func uint16ToBytes(n uint16) (b []byte) {
	b = make([]byte, 2)
	b[0] = byte(n)
	b[1] = byte(n >> 8)
	return
}

func bytesToUint16(b []byte) (n uint16) {
	n |= uint16(b[0])
	n |= uint16(b[1]) << 8
	return
}

func uint24ToBytes(n uint32) (b []byte) {
	b = make([]byte, 3)
	for i := uint8(0); i < 3; i++ {
		b[i] = byte(n >> (i * 8))
	}
	return
}

func readFixedLengthInteger(buf *bytes.Buffer, size int) (num uint64, err error) {
	var b byte
	num = 0
	if buf.Len() < size {
		return 0, io.EOF
	}
	for i := uint(0); i < uint(size); i++ {
		b, err = buf.ReadByte()
		num |= uint64(b) << (i * 8)
	}
	return
}

func bytesToUint32(b []byte) (n uint32) {
	for i := uint8(0); i < 4; i++ {
		n |= uint32(b[i]) << (i * 8)
	}
	return
}

func bytesToUint24(b []byte) (n uint32) {
	for i := uint8(0); i < 3; i++ {
		n |= uint32(b[i]) << (i * 8)
	}
	return
}

func uint32ToBytes(n uint32) (b []byte) {
	b = make([]byte, 4)
	for i := uint8(0); i < 4; i++ {
		b[i] = byte(n >> (i * 8))
	}
	return
}

func bytesToUint64(b []byte) (n uint64) {
	for i := uint8(0); i < 8; i++ {
		n |= uint64(b[i]) << (i * 8)
	}
	return
}

func uint64ToBytes(n uint64) (b []byte) {
	b = make([]byte, 8)
	for i := uint8(0); i < 8; i++ {
		b[i] = byte(n >> (i * 8))
	}
	return
}

func int64ToBytes(n int64) []byte {
	return uint64ToBytes(uint64(n))
}

func bytesToFloat32(b []byte) float32 {
	return math.Float32frombits(bytesToUint32(b))
}

func bytesToFloat64(b []byte) float64 {
	return math.Float64frombits(bytesToUint64(b))
}

func float64ToBytes(f float64) []byte {
	return uint64ToBytes(math.Float64bits(f))
}

func bytesToLengthCodedBinary(b []byte) (length uint64, n int, e error) {
	switch {

	// 0-250: value of first byte
	case b[0] <= 250:
		length = uint64(b[0])
		n = 1
		return

	// 251: NULL
	case b[0] == 251:
		length = 0
		n = 1
		return

	// 252: value of following 2
	case b[0] == 252:
		n = 3

	// 253: value of following 3
	case b[0] == 253:
		n = 4

	// 254: value of following 8
	case b[0] == 254:
		n = 9
	}

	if len(b) < n {
		e = io.EOF
		return
	}

	// get Length
	tmp := make([]byte, 8)
	copy(tmp, b[1:n])
	length = bytesToUint64(tmp)
	return
}

func lengthCodedBinaryToBytes(n uint64) (b []byte) {
	switch {

	case n <= 250:
		b = []byte{byte(n)}

	case n <= 0xffff:
		b = []byte{0xfc, byte(n), byte(n >> 8)}

	case n <= 0xffffff:
		b = []byte{0xfd, byte(n), byte(n >> 8), byte(n >> 16)}

	default:
		b = []byte{0xfe, byte(n), byte(n >> 8), byte(n >> 16), byte(n >> 24),
			byte(n >> 32), byte(n >> 40), byte(n >> 48), byte(n >> 56)}
	}
	return
}

func intToByteStr(i int64) (b []byte) {
	//tmp := make([]byte, 0)
	return strconv.AppendInt(b, i, 10)
}

func uintToByteStr(u uint64) (b []byte) {
	return strconv.AppendUint(b, u, 10)
}

func float32ToByteStr(f float32) (b []byte) {
	return strconv.AppendFloat(b, float64(f), 'f', -1, 32)
}

func float64ToByteStr(f float64) (b []byte) {
	return strconv.AppendFloat(b, f, 'f', -1, 64)
}

func read_uint64_be_by_bytes(b []byte) (n uint64) {
	for i := uint8(0); i < uint8(len(b)); i++ {
		n |= uint64(b[i]) << (uint64(i) * 8)
	}
	return n
}

// reserveBuffer checks cap(buf) and expand buffer to len(buf) + appendSize.
// If cap(buf) is not enough, reallocate new buffer.
func reserveBuffer(buf []byte, appendSize int) []byte {
	newSize := len(buf) + appendSize
	if cap(buf) < newSize {
		// Grow buffer exponentially
		newBuf := make([]byte, len(buf)*2+appendSize)
		copy(newBuf, buf)
		buf = newBuf
	}
	return buf[:newSize]
}

// escapeBytesBackslash escapes []byte with backslashes (\)
// This escapes the contents of a string (provided as []byte) by adding backslashes before special
// characters, and turning others into specific escape sequences, such as
// turning newlines into \n and null bytes into \0.
// https://github.com/mysql/mysql-server/blob/mysql-5.7.5/mysys/charset.c#L823-L932
func escapeBytesBackslash(buf, v []byte) []byte {
	pos := len(buf)
	buf = reserveBuffer(buf, len(v)*2)

	for _, c := range v {
		switch c {
		case '\x00':
			buf[pos+1] = '0'
			buf[pos] = '\\'
			pos += 2
		case '\n':
			buf[pos+1] = 'n'
			buf[pos] = '\\'
			pos += 2
		case '\r':
			buf[pos+1] = 'r'
			buf[pos] = '\\'
			pos += 2
		case '\x1a':
			buf[pos+1] = 'Z'
			buf[pos] = '\\'
			pos += 2
		case '\'':
			buf[pos+1] = '\''
			buf[pos] = '\\'
			pos += 2
		case '"':
			buf[pos+1] = '"'
			buf[pos] = '\\'
			pos += 2
		case '\\':
			buf[pos+1] = '\\'
			buf[pos] = '\\'
			pos += 2
		default:
			buf[pos] = c
			pos++
		}
	}

	return buf[:pos]
}

// escapeStringBackslash is similar to escapeBytesBackslash but for string.
func escapeStringBackslash(buf []byte, v string) []byte {
	pos := len(buf)
	buf = reserveBuffer(buf, len(v)*2)

	for i := 0; i < len(v); i++ {
		c := v[i]
		switch c {
		case '\x00':
			buf[pos+1] = '0'
			buf[pos] = '\\'
			pos += 2
		case '\n':
			buf[pos+1] = 'n'
			buf[pos] = '\\'
			pos += 2
		case '\r':
			buf[pos+1] = 'r'
			buf[pos] = '\\'
			pos += 2
		case '\x1a':
			buf[pos+1] = 'Z'
			buf[pos] = '\\'
			pos += 2
		case '\'':
			buf[pos+1] = '\''
			buf[pos] = '\\'
			pos += 2
		case '"':
			buf[pos+1] = '"'
			buf[pos] = '\\'
			pos += 2
		case '\\':
			buf[pos+1] = '\\'
			buf[pos] = '\\'
			pos += 2
		default:
			buf[pos] = c
			pos++
		}
	}

	return buf[:pos]
}

// escapeBytesQuotes escapes apostrophes in []byte by doubling them up.
// This escapes the contents of a string by doubling up any apostrophes that
// it contains. This is used when the NO_BACKSLASH_ESCAPES SQL_MODE is in
// effect on the server.
// https://github.com/mysql/mysql-server/blob/mysql-5.7.5/mysys/charset.c#L963-L1038
func escapeBytesQuotes(buf, v []byte) []byte {
	pos := len(buf)
	buf = reserveBuffer(buf, len(v)*2)

	for _, c := range v {
		if c == '\'' {
			buf[pos+1] = '\''
			buf[pos] = '\''
			pos += 2
		} else {
			buf[pos] = c
			pos++
		}
	}

	return buf[:pos]
}

// escapeStringQuotes is similar to escapeBytesQuotes but for string.
func escapeStringQuotes(buf []byte, v string) []byte {
	pos := len(buf)
	buf = reserveBuffer(buf, len(v)*2)

	for i := 0; i < len(v); i++ {
		c := v[i]
		if c == '\'' {
			buf[pos+1] = '\''
			buf[pos] = '\''
			pos += 2
		} else {
			buf[pos] = c
			pos++
		}
	}

	return buf[:pos]
}

func bitBytes2Int64(bitData []byte) (int64, error) {
	var resp string = ""
	var bit uint
	var end byte
	end = 8
	for k := 0; k < len(bitData); k++ {
		var current_byte []string
		var data int
		data = int(bitData[k])
		for bit = 0; bit < uint(end); bit++ {
			tmp := 1 << bit
			if (data & tmp) > 0 {
				current_byte = append(current_byte, "1")
			} else {
				current_byte = append(current_byte, "0")
			}
		}
		for k := len(current_byte); k > 0; k-- {
			resp += current_byte[k-1]
		}
	}
	return strconv.ParseInt(resp, 2, 64)
}

func appendArgsBufByBytes(buf []byte, v []byte, supportedBackslash bool) []byte {
	buf = append(buf, "_binary'"...)
	if supportedBackslash {
		buf = escapeBytesBackslash(buf, v)
	} else {
		buf = escapeBytesQuotes(buf, v)
	}
	buf = append(buf, '\'')
	return buf
}

func appendArgsBufByString(buf []byte, v string, supportedBackslash bool) []byte {
	buf = append(buf, '\'')
	if supportedBackslash {
		buf = escapeStringBackslash(buf, v)
	} else {
		buf = escapeStringQuotes(buf, v)
	}
	buf = append(buf, '\'')
	return buf
}