package mc

// Handles the connection with the memcached servers.

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"time"
)

// Tiered buffer pools for different body sizes to reduce allocations.
// Bodies smaller than or equal to the tier size use that pool.
// Bodies larger than 64KB are allocated directly (not pooled).
var (
	smallBodyPool  = sync.Pool{New: func() interface{} { return make([]byte, 256) }}
	mediumBodyPool = sync.Pool{New: func() interface{} { return make([]byte, 4096) }}
	largeBodyPool  = sync.Pool{New: func() interface{} { return make([]byte, 65536) }}
)

// getBodyBuffer returns a buffer of at least the requested size from the appropriate pool.
func getBodyBuffer(size int) []byte {
	switch {
	case size <= 256:
		return smallBodyPool.Get().([]byte)[:size]
	case size <= 4096:
		return mediumBodyPool.Get().([]byte)[:size]
	case size <= 65536:
		return largeBodyPool.Get().([]byte)[:size]
	default:
		return make([]byte, size) // Large bodies allocated directly
	}
}

// putBodyBuffer returns a buffer to the appropriate pool based on its capacity.
func putBodyBuffer(buf []byte) {
	c := cap(buf)
	switch {
	case c == 256:
		smallBodyPool.Put(buf[:c])
	case c == 4096:
		mediumBodyPool.Put(buf[:c])
	case c == 65536:
		largeBodyPool.Put(buf[:c])
		// Large buffers not returned to pool
	}
}

type mcConn interface {
	perform(m *msg) error
	performStats(m *msg) (McStats, error)
	quit(m *msg)
	backup(m *msg)
	restore(m *msg)
}

type connGen func(address, scheme, username, password string, config *Config) mcConn

// serverConn is a connection to a memcache server.
type serverConn struct {
	address   string
	scheme    string
	username  string
	password  string
	config    *Config
	conn      net.Conn
	rw        *bufio.ReadWriter
	opq       uint32
	backupMsg msg
	hdrBuf    [24]byte // pre-allocated buffer for headers
}

func newServerConn(address, scheme, username, password string, config *Config) mcConn {
	serverConn := &serverConn{
		address:  address,
		scheme:   scheme,
		username: username,
		password: password,
		config:   config,
	}
	return serverConn
}

func (sc *serverConn) perform(m *msg) error {
	// lazy connection
	if sc.conn == nil {
		err := sc.connect()
		if err != nil {
			return err
		}
	}
	return sc.sendRecv(m)
}

func (sc *serverConn) performStats(m *msg) (McStats, error) {
	// lazy connection
	if sc.conn == nil {
		err := sc.connect()
		if err != nil {
			return nil, err
		}
	}
	return sc.sendRecvStats(m)
}

func (sc *serverConn) quit(m *msg) {
	if sc.conn != nil {
		sc.sendRecv(m)

		if sc.conn != nil {
			sc.conn.Close()
			sc.conn = nil
			sc.rw = nil
		}
	}
}

func (sc *serverConn) connect() error {
	c, err := net.DialTimeout(sc.scheme, sc.address, sc.config.ConnectionTimeout)
	if err != nil {
		return wrapError(StatusNetworkError, err)
	}
	sc.conn = c
	if sc.scheme == "tcp" {
		tcpConn, ok := c.(*net.TCPConn)
		if !ok {
			return &Error{StatusNetworkError, "Cannot convert into TCP connection", nil}
		}

		tcpConn.SetKeepAlive(sc.config.TcpKeepAlive)
		tcpConn.SetKeepAlivePeriod(sc.config.TcpKeepAlivePeriod)
		tcpConn.SetNoDelay(sc.config.TcpNoDelay)
	}

	sc.rw = bufio.NewReadWriter(bufio.NewReader(c), bufio.NewWriter(c))

	// authenticate
	err = sc.auth()
	if err != nil {
		// Error, except if the server doesn't support authentication
		mErr := err.(*Error)
		if mErr.Status != StatusUnknownCommand {
			if sc.conn != nil {
				sc.conn.Close()
				sc.conn = nil
				sc.rw = nil
			}
			return err
		}
	}
	return nil
}

// Auth performs SASL authentication (using the PLAIN method) with the server.
func (sc *serverConn) auth() error {
	if len(sc.username) == 0 && len(sc.password) == 0 {
		return nil
	}
	s, err := sc.authList()
	if err != nil {
		return err
	}

	switch {
	case strings.Contains(s, "PLAIN"):
		return sc.authPlain()
	}

	return &Error{StatusAuthUnknown, fmt.Sprintf("mc: unknown auth types %q", s), nil}
}

// authList runs the SASL authentication list command with the server to
// retrieve the list of support authentication mechanisms.
func (sc *serverConn) authList() (string, error) {
	m := &msg{
		header: header{
			Op: opAuthList,
		},
	}

	err := sc.sendRecv(m)
	return m.val, err
}

// authPlain performs SASL authentication using the PLAIN method.
func (sc *serverConn) authPlain() error {
	m := &msg{
		header: header{
			Op: opAuthStart,
		},

		key: "PLAIN",
		val: fmt.Sprintf("\x00%s\x00%s", sc.username, sc.password),
	}

	return sc.sendRecv(m)
}

// sendRecv sends and receives a complete memcache request/response exchange.
func (sc *serverConn) sendRecv(m *msg) error {
	err := sc.send(m)
	if err != nil {
		sc.resetConn(err)
		return err
	}
	err = sc.recv(m)
	if err != nil {
		sc.resetConn(err)
		return err
	}
	return nil
}

// sendRecvStats
func (sc *serverConn) sendRecvStats(m *msg) (stats McStats, err error) {
	err = sc.send(m)
	if err != nil {
		sc.resetConn(err)
		return
	}

	// collect all statistics
	stats = make(map[string]string)
	for {
		err = sc.recv(m)
		// error or termination message
		if err != nil || m.KeyLen == 0 {
			if err != nil {
				sc.resetConn(err)
			}
			return
		}
		stats[m.key] = m.val
	}
}

// send sends a request to the memcache server.
func (sc *serverConn) send(m *msg) error {
	m.Magic = magicSend
	m.ExtraLen = sizeOfExtras(m.iextras)
	m.KeyLen = uint16(len(m.key))
	m.BodyLen = uint32(m.ExtraLen) + uint32(m.KeyLen) + uint32(len(m.val))
	m.Opaque = sc.opq
	sc.opq++

	// Header
	sc.hdrBuf[0] = uint8(m.Magic)
	sc.hdrBuf[1] = uint8(m.Op)
	binary.BigEndian.PutUint16(sc.hdrBuf[2:], m.KeyLen)
	sc.hdrBuf[4] = m.ExtraLen
	sc.hdrBuf[5] = m.DataType
	binary.BigEndian.PutUint16(sc.hdrBuf[6:], m.ResvOrStatus)
	binary.BigEndian.PutUint32(sc.hdrBuf[8:], m.BodyLen)
	binary.BigEndian.PutUint32(sc.hdrBuf[12:], m.Opaque)
	binary.BigEndian.PutUint64(sc.hdrBuf[16:], m.CAS)

	// Make sure write does not block forever
	sc.conn.SetWriteDeadline(time.Now().Add(sc.config.ConnectionTimeout))

	if _, err := sc.rw.Write(sc.hdrBuf[:]); err != nil {
		return wrapError(StatusNetworkError, err)
	}

	for _, e := range m.iextras {
		var err error
		switch v := e.(type) {
		case uint8:
			err = sc.rw.WriteByte(v)
		case uint16:
			var b [2]byte
			binary.BigEndian.PutUint16(b[:], v)
			_, err = sc.rw.Write(b[:])
		case uint32:
			var b [4]byte
			binary.BigEndian.PutUint32(b[:], v)
			_, err = sc.rw.Write(b[:])
		case uint64:
			var b [8]byte
			binary.BigEndian.PutUint64(b[:], v)
			_, err = sc.rw.Write(b[:])
		default:
			panic(fmt.Sprintf("mc: unknown extra type (%T)", e))
		}
		if err != nil {
			return wrapError(StatusNetworkError, err)
		}
	}

	if len(m.key) > 0 {
		if _, err := io.WriteString(sc.rw, m.key); err != nil {
			return wrapError(StatusNetworkError, err)
		}
	}

	if len(m.val) > 0 {
		if _, err := io.WriteString(sc.rw, m.val); err != nil {
			return wrapError(StatusNetworkError, err)
		}
	}

	if err := sc.rw.Flush(); err != nil {
		return wrapError(StatusNetworkError, err)
	}

	return nil
}

// recv receives a memcached response. It takes a msg into which to store the
// response.
func (sc *serverConn) recv(m *msg) error {
	// Make sure read does not block forever
	sc.conn.SetReadDeadline(time.Now().Add(sc.config.ConnectionTimeout))

	// Read Header
	if _, err := io.ReadFull(sc.rw, sc.hdrBuf[:]); err != nil {
		return wrapError(StatusNetworkError, err)
	}

	// Parse Header
	m.header.Magic = magicCode(sc.hdrBuf[0])
	m.header.Op = opCode(sc.hdrBuf[1])
	m.header.KeyLen = binary.BigEndian.Uint16(sc.hdrBuf[2:])
	m.header.ExtraLen = sc.hdrBuf[4]
	m.header.DataType = sc.hdrBuf[5]
	m.header.ResvOrStatus = binary.BigEndian.Uint16(sc.hdrBuf[6:])
	m.header.BodyLen = binary.BigEndian.Uint32(sc.hdrBuf[8:])
	m.header.Opaque = binary.BigEndian.Uint32(sc.hdrBuf[12:])
	m.header.CAS = binary.BigEndian.Uint64(sc.hdrBuf[16:])

	// Read Body
	// Use pooled buffer for the body. Since we convert to string (which copies),
	// we can safely return the buffer to the pool after.
	body := getBodyBuffer(int(m.BodyLen))
	defer putBodyBuffer(body)
	if _, err := io.ReadFull(sc.rw, body); err != nil {
		return wrapError(StatusNetworkError, err)
	}

	buf := body // alias for slicing

	// Read Extras
	if m.ResvOrStatus == 0 && m.ExtraLen > 0 {
		if len(buf) < int(m.ExtraLen) {
			return wrapError(StatusNetworkError, io.ErrUnexpectedEOF)
		}
		extrasBuf := buf[:m.ExtraLen]
		buf = buf[m.ExtraLen:]

		offset := 0
		for _, e := range m.oextras {
			switch ptr := e.(type) {
			case *uint8:
				if offset+1 > len(extrasBuf) {
					return wrapError(StatusNetworkError, io.ErrUnexpectedEOF)
				}
				*ptr = extrasBuf[offset]
				offset += 1
			case *uint16:
				if offset+2 > len(extrasBuf) {
					return wrapError(StatusNetworkError, io.ErrUnexpectedEOF)
				}
				*ptr = binary.BigEndian.Uint16(extrasBuf[offset:])
				offset += 2
			case *uint32:
				if offset+4 > len(extrasBuf) {
					return wrapError(StatusNetworkError, io.ErrUnexpectedEOF)
				}
				*ptr = binary.BigEndian.Uint32(extrasBuf[offset:])
				offset += 4
			case *uint64:
				if offset+8 > len(extrasBuf) {
					return wrapError(StatusNetworkError, io.ErrUnexpectedEOF)
				}
				*ptr = binary.BigEndian.Uint64(extrasBuf[offset:])
				offset += 8
			default:
				// Fallback to binary.Read if we missed something, though likely unneeded
				// But we are reading from slice now, not reader.
				// For now assuming we covered standard extras.
				return wrapError(StatusNetworkError, fmt.Errorf("mc: unknown extra type in response %T", e))
			}
		}
	}

	// Read Key
	if len(buf) < int(m.KeyLen) {
		return wrapError(StatusNetworkError, io.ErrUnexpectedEOF)
	}
	m.key = string(buf[:m.KeyLen])
	buf = buf[m.KeyLen:]

	// Read Value (remaining)
	m.val = string(buf)

	return newError(m.ResvOrStatus)
}

// sizeOfExtras returns the size of the extras field for the memcache request.
func sizeOfExtras(extras []interface{}) (l uint8) {
	for _, e := range extras {
		switch e.(type) {
		case uint8:
			l += 1
		case uint16:
			l += 2
		case uint32:
			l += 4
		case uint64:
			l += 8
		default:
			panic(fmt.Sprintf("mc: unknown extra type (%T)", e))
		}
	}
	return
}

// resetConn destroy connection if a network error occurred. serverConn will
// reconnect on next usage.
func (sc *serverConn) resetConn(err error) {
	if err.(*Error).Status == StatusNetworkError {
		if sc.conn != nil {
			sc.conn.Close()
			sc.conn = nil
			sc.rw = nil
		}
	}
}

func (sc *serverConn) backup(m *msg) {
	backupMsg(m, &sc.backupMsg)
}

func backupMsg(m *msg, backupMsg *msg) {
	backupMsg.key = m.key
	backupMsg.val = m.val
	backupMsg.header = m.header // Copy entire struct at once

	// Reuse slice capacity if possible to avoid allocation
	if cap(backupMsg.iextras) >= len(m.iextras) {
		backupMsg.iextras = backupMsg.iextras[:len(m.iextras)]
	} else {
		backupMsg.iextras = make([]interface{}, len(m.iextras))
	}
	copy(backupMsg.iextras, m.iextras)

	if cap(backupMsg.oextras) >= len(m.oextras) {
		backupMsg.oextras = backupMsg.oextras[:len(m.oextras)]
	} else {
		backupMsg.oextras = make([]interface{}, len(m.oextras))
	}
	copy(backupMsg.oextras, m.oextras)
}

func (sc *serverConn) restore(m *msg) {
	restoreMsg(m, &sc.backupMsg)
}

func restoreMsg(m *msg, backupMsg *msg) {
	m.key = backupMsg.key
	m.val = backupMsg.val
	m.header = backupMsg.header // Copy entire struct at once

	// Reuse slice capacity if possible to avoid allocation
	if cap(m.iextras) >= len(backupMsg.iextras) {
		m.iextras = m.iextras[:len(backupMsg.iextras)]
	} else {
		m.iextras = make([]interface{}, len(backupMsg.iextras))
	}
	copy(m.iextras, backupMsg.iextras)

	if cap(m.oextras) >= len(backupMsg.oextras) {
		m.oextras = m.oextras[:len(backupMsg.oextras)]
	} else {
		m.oextras = make([]interface{}, len(backupMsg.oextras))
	}
	copy(m.oextras, backupMsg.oextras)
}
