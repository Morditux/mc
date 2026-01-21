package mc

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"net"
	"testing"
	"time"
)

// MockNetConn implements net.Conn using bytes.Buffer for testing
type MockNetConn struct {
	ReadBuf  *bytes.Buffer
	WriteBuf *bytes.Buffer
}

func NewMockNetConn() *MockNetConn {
	return &MockNetConn{
		ReadBuf:  new(bytes.Buffer),
		WriteBuf: new(bytes.Buffer),
	}
}

func (m *MockNetConn) Read(b []byte) (n int, err error) {
	return m.ReadBuf.Read(b)
}

func (m *MockNetConn) Write(b []byte) (n int, err error) {
	return m.WriteBuf.Write(b)
}

func (m *MockNetConn) Close() error {
	return nil
}

func (m *MockNetConn) LocalAddr() net.Addr {
	return nil
}

func (m *MockNetConn) RemoteAddr() net.Addr {
	return nil
}

func (m *MockNetConn) SetDeadline(t time.Time) error {
	return nil
}

func (m *MockNetConn) SetReadDeadline(t time.Time) error {
	return nil
}

func (m *MockNetConn) SetWriteDeadline(t time.Time) error {
	return nil
}

func TestServerConn_Send(t *testing.T) {
	mockConn := NewMockNetConn()
	sc := &serverConn{
		config: DefaultConfig(),
		conn:   mockConn,
		rw:     bufio.NewReadWriter(bufio.NewReader(mockConn), bufio.NewWriter(mockConn)),
	}

	key := "foo"
	val := "bar"
	flags := uint32(0xdeadbeef)
	exp := uint32(3600)

	m := &msg{
		header: header{
			Op: opSet,
		},
		iextras: []interface{}{flags, exp},
		key:     key,
		val:     val,
	}

	err := sc.send(m)
	if err != nil {
		t.Fatalf("send failed: %v", err)
	}

	// Verify the written bytes
	// Header (24) + Extras (8) + Key (3) + Value (3) = 38 bytes
	expectedLen := 24 + 8 + 3 + 3
	if mockConn.WriteBuf.Len() != expectedLen {
		t.Errorf("Expected %d bytes, got %d", expectedLen, mockConn.WriteBuf.Len())
	}

	bytes := mockConn.WriteBuf.Bytes()

	// Check Magic
	if bytes[0] != uint8(magicSend) {
		t.Errorf("Wrong magic: %x", bytes[0])
	}
	// Check Op
	if bytes[1] != uint8(opSet) {
		t.Errorf("Wrong op: %x", bytes[1])
	}
	// Check Key Length
	klen := binary.BigEndian.Uint16(bytes[2:4])
	if klen != 3 {
		t.Errorf("Wrong key length: %d", klen)
	}
	// Check Extra Length
	if bytes[4] != 8 {
		t.Errorf("Wrong extra length: %d", bytes[4])
	}
	// Check Body Length
	blen := binary.BigEndian.Uint32(bytes[8:12])
	if blen != uint32(8+3+3) {
		t.Errorf("Wrong body length: %d", blen)
	}

	// Check Extras
	gotFlags := binary.BigEndian.Uint32(bytes[24:28])
	if gotFlags != flags {
		t.Errorf("Wrong flags: %x", gotFlags)
	}
	gotExp := binary.BigEndian.Uint32(bytes[28:32])
	if gotExp != exp {
		t.Errorf("Wrong exp: %x", gotExp)
	}

	// Check Key
	gotKey := string(bytes[32:35])
	if gotKey != key {
		t.Errorf("Wrong key: %s", gotKey)
	}

	// Check Value
	gotVal := string(bytes[35:])
	if gotVal != val {
		t.Errorf("Wrong val: %s", gotVal)
	}
}

func TestServerConn_Recv(t *testing.T) {
	mockConn := NewMockNetConn()
	sc := &serverConn{
		config: DefaultConfig(),
		conn:   mockConn,
		rw:     bufio.NewReadWriter(bufio.NewReader(mockConn), bufio.NewWriter(mockConn)),
	}

	// Construct a response
	// Header + Extras + Key + Value
	key := "foo"
	val := "world"
	flags := uint32(0xCAFEBABE)

	// Extras size = 4 (flags)
	extraLen := 4
	keyLen := len(key)
	valLen := len(val)
	bodyLen := extraLen + keyLen + valLen

	buf := new(bytes.Buffer)
	// Header
	buf.WriteByte(uint8(magicRecv)) // Magic
	buf.WriteByte(uint8(opGet))     // Op
	binary.Write(buf, binary.BigEndian, uint16(keyLen))
	buf.WriteByte(uint8(extraLen))
	buf.WriteByte(0) // DataType
	binary.Write(buf, binary.BigEndian, uint16(StatusOK))
	binary.Write(buf, binary.BigEndian, uint32(bodyLen))
	binary.Write(buf, binary.BigEndian, uint32(123)) // Opaque
	binary.Write(buf, binary.BigEndian, uint64(999)) // CAS

	// Extras
	binary.Write(buf, binary.BigEndian, flags)

	// Key
	buf.WriteString(key)

	// Value
	buf.WriteString(val)

	// Write to mock connection
	mockConn.ReadBuf.Write(buf.Bytes())

	// Prepare msg to receive
	var gotFlags uint32
	m := &msg{
		oextras: []interface{}{&gotFlags},
	}

	err := sc.recv(m)
	if err != nil {
		t.Fatalf("recv failed: %v", err)
	}

	if m.key != key {
		t.Errorf("Expected key %s, got %s", key, m.key)
	}
	if m.val != val {
		t.Errorf("Expected val %s, got %s", val, m.val)
	}
	if gotFlags != flags {
		t.Errorf("Expected flags %x, got %x", flags, gotFlags)
	}
	if m.header.CAS != 999 {
		t.Errorf("Expected CAS 999, got %d", m.header.CAS)
	}
	if m.header.Opaque != 123 {
		t.Errorf("Expected Opaque 123, got %d", m.header.Opaque)
	}
}
