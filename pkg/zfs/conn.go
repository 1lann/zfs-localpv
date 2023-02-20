package zfs

import (
	"net"
	"sync"
	"time"
)

// ConnWithErr is a net.Conn wrapper that tracks read and write errors.
type ConnWithErr struct {
	conn     net.Conn
	readErr  error
	writeErr error
	mu       sync.Mutex
}

// NewConnWithErr wraps a net.Conn with a ConnWithErr.
func NewConnWithErr(conn net.Conn) *ConnWithErr {
	return &ConnWithErr{
		conn: conn,
	}
}

// Read reads data from the connection.
func (c *ConnWithErr) Read(b []byte) (int, error) {
	n, err := c.conn.Read(b)
	if err != nil {
		c.mu.Lock()
		c.readErr = err
		c.mu.Unlock()
	}
	return n, err
}

// Write writes data to the connection.
func (c *ConnWithErr) Write(b []byte) (int, error) {
	n, err := c.conn.Write(b)
	if err != nil {
		c.mu.Lock()
		c.writeErr = err
		c.mu.Unlock()
	}
	return n, err
}

// Err returns any read or write error that has occurred.
func (c *ConnWithErr) Err() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.readErr != nil {
		return c.readErr
	}
	return c.writeErr
}

// Close closes the connection.
func (c *ConnWithErr) Close() error {
	return c.conn.Close()
}

// LocalAddr returns the local network address.
func (c *ConnWithErr) LocalAddr() net.Addr {
	return c.conn.LocalAddr()
}

// RemoteAddr returns the remote network address.
func (c *ConnWithErr) RemoteAddr() net.Addr {
	return c.conn.RemoteAddr()
}

// SetDeadline sets the read and write deadlines associated.
func (c *ConnWithErr) SetDeadline(t time.Time) error {
	return c.conn.SetDeadline(t)
}

// SetReadDeadline sets the deadline for future Read calls.
func (c *ConnWithErr) SetReadDeadline(t time.Time) error {
	return c.conn.SetReadDeadline(t)
}

// SetWriteDeadline sets the deadline for future Write calls.
func (c *ConnWithErr) SetWriteDeadline(t time.Time) error {
	return c.conn.SetWriteDeadline(t)
}
