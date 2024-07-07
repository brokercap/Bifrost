package mysql

import (
	"bytes"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha1"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"fmt"
	"log"
)

/* get auth password
 */
func (mc *mysqlConn) getAuthPasswordScrambleBuff() (result []byte, addPasswordIsNull bool, e error) {
	switch mc.cfg.authPluginName {
	case AUTH_NATIVE_PASSWORD:
		return AuthNavtivePassword(mc.server.scrambleBuff, []byte(mc.cfg.passwd)), false, nil
	case AUTH_CACHING_SHA2_PASSWORD:
		return AuthCachingSha2Password(mc.server.scrambleBuff, []byte(mc.cfg.passwd)), false, nil
	case AUTH_SHA256_PASSWORD:
		if len(mc.cfg.passwd) == 0 {
			return nil, true, nil
		}
		if mc.cfg.tlsConfig != nil || mc.cfg.net == "unix" {
			// write cleartext auth packet
			// see: https://dev.mysql.com/doc/refman/8.0/en/sha256-pluggable-authentication.html
			return []byte(mc.cfg.passwd), true, nil
		} else {
			// request public key from server
			// see: https://dev.mysql.com/doc/internals/en/public-key-retrieval.html
			return []byte{1}, false, nil
		}
	default:
		return nil, false, errors.New(mc.cfg.authPluginName + " not supported yet!")
	}
	return
}

// NativePassword
// Encrypt password using 4.1+ method
// http://forge.mysql.com/wiki/MySQL_Internals_ClientServer_Protocol#4.1_and_later
func AuthNavtivePassword(scramble, password []byte) (result []byte) {
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

// CalcCachingSha2Password: Hash password using MySQL 8+ method (SHA256)
func AuthCachingSha2Password(scramble, password []byte) []byte {
	if len(password) == 0 {
		return nil
	}

	// XOR(SHA256(password), SHA256(SHA256(SHA256(password)), scramble))

	crypt := sha256.New()
	crypt.Write([]byte(password))
	message1 := crypt.Sum(nil)

	crypt.Reset()
	crypt.Write(message1)
	message1Hash := crypt.Sum(nil)

	crypt.Reset()
	crypt.Write(message1Hash)
	crypt.Write(scramble)
	message2 := crypt.Sum(nil)

	for i := range message1 {
		message1[i] ^= message2[i]
	}

	return message1
}

/******************************************************************************
*                           Initialisation Process                            *
******************************************************************************/

/*
	Handshake Initialization Packet
	Bytes                        Name
	-----                        ----
	1                            protocol_version
	n (Null-Terminated String)   server_version
	4                            thread_id
	8                            scramble_buff
	1                            (filler) always 0x00
	2                            server_capabilities
	1                            server_language
	2                            server_status
	2                            server capabilities (two upper bytes)
	1                            length of the scramble

10                            (filler)  always 0

	n                            rest of the plugin provided data (at least 12 bytes)
	1                            \0 byte, terminating the second part of a scramble
*/
func (mc *mysqlConn) readInitPacket() (e error) {
	data, e := mc.readPacket()
	if e != nil {
		return
	}

	mc.server = new(serverSettings)

	// Position
	pos := 0

	// Protocol version [8 bit uint]
	mc.server.protocol = data[pos]
	if mc.server.protocol < MIN_PROTOCOL_VERSION {
		e = fmt.Errorf(
			"Unsupported MySQL Protocol Version %d. Protocol Version %d or higher is required",
			mc.server.protocol,
			MIN_PROTOCOL_VERSION)
	}
	pos++

	// Server version [null terminated string]
	slice, err := readSlice(data[pos:], 0x00)
	if err != nil {
		return
	}
	mc.server.version = string(slice)
	pos += len(slice) + 1

	// Thread id [32 bit uint]
	mc.server.threadID = bytesToUint32(data[pos : pos+4])
	pos += 4

	// First part of scramble buffer [8 bytes]
	mc.server.scrambleBuff = make([]byte, 8)
	mc.server.scrambleBuff = data[pos : pos+8]
	pos += 9

	// Server capabilities [16 bit uint]
	mc.server.flags = ClientFlag(bytesToUint16(data[pos : pos+2]))
	// check protocol
	if mc.server.flags&CLIENT_PROTOCOL_41 == 0 {
		e = errors.New("MySQL-Server does not support required Protocol 41+")
	}
	if mc.server.flags&CLIENT_SSL == 0 && mc.cfg.tlsConfig != nil {
		return errors.New("the MySQL Server does not support TLS required by the client")
	}
	pos += 2

	if len(data) > pos {
		// Server language [8 bit uint]
		mc.server.charset = data[pos]
		pos++

		//Server status
		mc.status = bytesToUint16(data[pos : pos+2])
		pos += 2

		// capability flags (upper 2 bytes)
		mc.server.flags = ClientFlag(uint32(bytesToUint16(data[pos:pos+2]))<<16 | uint32(mc.server.flags))
		pos += 2
		var authPluginDataPartLen int
		if (mc.server.flags & CLIENT_PLUGIN_AUTH) != 0 {
			authPluginDataPartLen = int(data[pos])
		}
		// auth data len or [00]
		pos++

		// skip reserved (all [00])
		pos += 10

		var authPluginDataPart2Len int
		authPluginDataPart2Len = authPluginDataPartLen - 8
		if authPluginDataPart2Len < 13 {
			authPluginDataPart2Len = 13
		}
		mc.server.scrambleBuff = append(mc.server.scrambleBuff, data[pos:pos+authPluginDataPart2Len-1]...)
		pos += authPluginDataPart2Len
		// auth plugin
		if end := bytes.IndexByte(data[pos:], 0x00); end != -1 {
			mc.cfg.authPluginName = string(data[pos : pos+end])
		} else {
			mc.cfg.authPluginName = string(data[pos:])
		}
	}

	if mc.cfg.authPluginName == "" {
		mc.cfg.authPluginName = AUTH_NATIVE_PASSWORD
	}
	return
}

/*
	Client Authentication Packet

Bytes                        Name
-----                        ----
4                            client_flags
4                            max_packet_size
1                            charset_number
23                           (filler) always 0x00...
n (Null-Terminated String)   user
n (Length Coded Binary)      scramble_buff (1 + x bytes)
n (Null-Terminated String)   databasename (optional)
*/
func (mc *mysqlConn) writeAuthPacket() (e error) {
	// Adjust client flags based on server support
	clientFlags := uint32(CLIENT_MULTI_STATEMENTS |
		// CLIENT_MULTI_RESULTS |
		CLIENT_PROTOCOL_41 |
		CLIENT_SECURE_CONN |
		CLIENT_LONG_PASSWORD |
		CLIENT_TRANSACTIONS | CLIENT_PLUGIN_AUTH | mc.server.flags&CLIENT_LONG_FLAG)

	// User Password
	var scrambleBuff []byte
	var addPasswordIsNull bool
	scrambleBuff, addPasswordIsNull, e = mc.getAuthPasswordScrambleBuff()
	if e != nil {
		return
	}

	// encode length of the auth plugin data
	// here we use the Length-Encoded-Integer(LEI) as the data length may not fit into one byte
	// see: https://dev.mysql.com/doc/internals/en/integer.html#length-encoded-integer
	authRespLEI := lengthCodedBinaryToBytes(uint64(len(scrambleBuff)))
	if len(authRespLEI) > 1 {
		// if the length can not be written in 1 byte, it must be written as a
		// length encoded integer
		clientFlags |= uint32(CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA)
	}

	// To specify a db name
	if len(mc.cfg.dbname) > 0 {
		clientFlags |= uint32(CLIENT_CONNECT_WITH_DB)
	}
	// To enable TLS / SSL
	if mc.cfg.tlsConfig != nil {
		clientFlags |= uint32(CLIENT_SSL)
	}
	// Calculate packet length and make buffer with that size
	pktLen := 4 + 4 + 1 + 23 + len(mc.cfg.user) + 1 + len(authRespLEI) + len(scrambleBuff) + len([]byte(mc.cfg.authPluginName)) + 1
	if n := len(mc.cfg.dbname); n > 0 {
		pktLen += n + 1
	}
	// sha256 password 是不会传输密码的
	if addPasswordIsNull {
		pktLen++
	}
	data := make([]byte, 0, pktLen)

	// ClientFlags
	data = append(data, uint32ToBytes(clientFlags)...)

	// MaxPacketSize
	data = append(data, uint32ToBytes(MAX_PACKET_SIZE)...)

	// Charset
	data = append(data, mc.server.charset)

	// Filler
	data = append(data, make([]byte, 23)...)

	if mc.cfg.tlsConfig != nil {
		tlsPacketHeaderLen := 4 + 4 + 1 + 23
		tlsPacketHeader := make([]byte, 0, tlsPacketHeaderLen+4)
		tlsPacketHeader = append(tlsPacketHeader, uint24ToBytes(uint32(tlsPacketHeaderLen))...)
		tlsPacketHeader = append(tlsPacketHeader, mc.sequence)
		tlsPacketHeader = append(tlsPacketHeader, data[:tlsPacketHeaderLen]...)
		if err := mc.writePacket(&tlsPacketHeader); err != nil {
			return err
		}
		// Switch to TLS
		tlsConn := tls.Client(mc.netConn, mc.cfg.tlsConfig)
		if err := tlsConn.Handshake(); err != nil {
			return err
		}
		mc.initConn(tlsConn)
	}

	// User
	if len(mc.cfg.user) > 0 {
		data = append(data, []byte(mc.cfg.user)...)
	}

	// Null-Terminator
	data = append(data, 0x0)

	data = append(data, authRespLEI...)
	data = append(data, scrambleBuff...)
	// sha256 password 等加密模式,是不传输的
	if addPasswordIsNull {
		data = append(data, 0x0)
	}

	// Databasename
	if len(mc.cfg.dbname) > 0 {
		data = append(data, []byte(mc.cfg.dbname)...)
		// Null-Terminator
		data = append(data, 0x0)
	}

	data = append(data, []byte(mc.cfg.authPluginName)...)
	data = append(data, 0x00)

	data0 := make([]byte, 0, len(data)+4)
	//Add the packet header
	data0 = append(data0, uint24ToBytes(uint32(len(data)))...)
	data0 = append(data0, mc.sequence)
	data0 = append(data0, data...)
	// Send Auth packet
	return mc.writePacket(&data0)
}

func (mc *mysqlConn) handleAuthResult() error {
	data, switchToPlugin, err := mc.readAuthResult()
	if err != nil {
		return err
	}
	// handle auth switch, only support 'sha256_password', and 'caching_sha2_password'
	// eg: mysql 8.0 default caching_sha2_password, but user use native_password
	if switchToPlugin != "" {
		//fmt.Printf("now switching auth plugin to '%s'\n", switchToPlugin)
		if data == nil {
			data = mc.server.scrambleBuff
		} else {
			// if switchToPlugin == native_password, len(data) == 21,but getAuthPasswordScrambleBuff only use 20 byte
			// ,becuase scrambleBuff include one Terminator
			// See: http://dev.mysql.com/doc/internals/en/connection-phase-packets.html#packet-Protocol::Handshake
			mc.server.scrambleBuff = data[:len(data)-1]
		}
		mc.cfg.authPluginName = switchToPlugin
		scrambleBuff, addPasswordIsNull, err := mc.getAuthPasswordScrambleBuff()
		if err != nil {
			return err
		}
		err = mc.WriteAuthSwitchPacket(scrambleBuff, addPasswordIsNull)
		if err != nil {
			return err
		}
		// Read Result Packet
		data, switchToPlugin, err = mc.readAuthResult()
		if err != nil {
			return err
		}
		// Do not allow to change the auth plugin more than once
		if switchToPlugin != "" {
			return errors.New("can't switch auth plugin more than once")
		}
	}

	switch mc.cfg.authPluginName {
	case AUTH_CACHING_SHA2_PASSWORD:
		if data == nil {
			return nil // auth already succeeded
		}
		switch data[0] {
		case 3:
			if err = mc.readResultOK(); err == nil {
				return nil // auth successful
			}
		case 4:
			if mc.cfg.tlsConfig != nil {
				return errors.New("Invalid packet tls and unix socket")
			} else {
				if err = mc.WritePublicKeyAuthPacket(); err != nil {
					log.Printf("WritePublicKeyAuthPacket err:%v", err)
					return err
				}
			}
		default:
			return errors.New("Invalid packet")
		}
	case AUTH_NATIVE_PASSWORD:
		if data == nil {
			return nil // auth already succeeded
		}
		if err = mc.readResultOK(); err == nil {
			return nil // auth successful
		} else {
			return err
		}
	case AUTH_SHA256_PASSWORD:
		if data == nil {
			return nil // auth already succeeded
		}
		block, _ := pem.Decode(data)
		pub, err := x509.ParsePKIXPublicKey(block.Bytes)
		if err != nil {
			return err
		}
		// send encrypted password
		err = mc.WriteEncryptedByPublicKey(pub.(*rsa.PublicKey))
		if err != nil {
			return err
		}
		err = mc.readResultOK()
		return err
	default:
		return errors.New("not support " + mc.cfg.authPluginName)
	}
	return nil
}

func (mc *mysqlConn) readAuthResult() ([]byte, string, error) {
	data, err := mc.readPacket()
	if err != nil {
		return nil, "", err
	}
	// see: https://insidemysql.com/preparing-your-community-connector-for-mysql-8-part-2-sha256/
	// packet indicator
	switch data[0] {
	case 0:
		err := mc.handleOkPacket(data)
		return nil, "", err
	case 1:
		return data[1:], "", err

	case 254:
		// server wants to switch auth
		if len(data) < 1 {
			// https://dev.mysql.com/doc/internals/en/connection-phase-packets.html#packet-Protocol::OldAuthSwitchRequest
			return nil, AUTH_MYSQL_OLD_PASSWORD, nil
		}
		pluginEndIndex := bytes.IndexByte(data, 0x00)
		if pluginEndIndex < 0 {
			return nil, "", errors.New("Invalid packet")
		}
		plugin := string(data[1:pluginEndIndex])
		authData := data[pluginEndIndex+1:]
		return authData, plugin, nil

	default: // Error otherwise
		return nil, "", mc.handleErrorPacket(data)
	}
}

func (mc *mysqlConn) EncryptPasswordByPublicKey(password string, seed []byte, pub *rsa.PublicKey) ([]byte, error) {
	plain := make([]byte, len(password)+1)
	copy(plain, password)
	for i := range plain {
		j := i % len(seed)
		plain[i] ^= seed[j]
	}
	sha1v := sha1.New()
	return rsa.EncryptOAEP(sha1v, rand.Reader, pub, plain, nil)
}

func (mc *mysqlConn) WriteEncryptedByPublicKey(publicKey *rsa.PublicKey) error {
	EncryptData, err := mc.EncryptPasswordByPublicKey(mc.cfg.passwd, mc.server.scrambleBuff, publicKey)
	if err != nil {
		return err
	}
	err = mc.WriteAuthSwitchPacket(EncryptData, false)
	return err
}

func (mc *mysqlConn) WriteAuthSwitchPacket(packetData []byte, addPasswordIsNull bool) error {
	var dataLen int = len(packetData) + 4
	if addPasswordIsNull {
		dataLen++
	}
	data0 := make([]byte, 0, dataLen)
	data0 = append(data0, uint24ToBytes(uint32(len(packetData)))...)
	data0 = append(data0, mc.sequence)
	data0 = append(data0, packetData...)
	if addPasswordIsNull {
		data0 = append(data0, 0x0)
	}
	err := mc.writePacket(&data0)
	return err
}

func (mc *mysqlConn) WritePublicKeyAuthPacket() error {
	// request public key
	data0 := make([]byte, 0)
	data0 = append(data0, uint24ToBytes(1)...)
	data0 = append(data0, mc.sequence)
	data0 = append(data0, byte(2))
	if err := mc.writePacket(&data0); err != nil {
		return fmt.Errorf("WritePacket(single byte) failed. err: %v", err)
	}
	data, err := mc.readPacket()
	//log.Fatal("ssssssss")
	if err != nil {
		return fmt.Errorf("ReadPacket failed. err: %v", err)
	}
	block, _ := pem.Decode(data[1:])
	if block == nil {
		log.Println("block is null")
	}
	pub, err := x509.ParsePKIXPublicKey(block.Bytes)
	if err != nil {
		return fmt.Errorf("x509.ParsePKIXPublicKey failed, err:%v", err)
	}

	plain := make([]byte, len(mc.cfg.passwd)+1)
	copy(plain, mc.cfg.passwd)
	for i := range plain {
		j := i % len(mc.server.scrambleBuff)
		plain[i] ^= mc.server.scrambleBuff[j]
	}
	sha1v := sha1.New()
	enc, _ := rsa.EncryptOAEP(sha1v, rand.Reader, pub.(*rsa.PublicKey), plain, nil)
	data2 := make([]byte, 0, 4+len(enc))
	data2 = append(data2, uint24ToBytes(uint32(len(enc)))...)
	data2 = append(data2, mc.sequence)
	data2 = append(data2, enc...)

	err = mc.writePacket(&data2)
	return err
}
