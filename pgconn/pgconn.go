package pgconn

import (
	"context"
	"errors"
	"net"
	"sync"
	"time"
)

const (
	TxStatusIdle          byte = 'I'
	TxStatusInTransaction byte = 'T'
	TxStatusInError       byte = 'E'
)

var (
	ErrConnectionClosed = errors.New("connection is closed")
)

type PgConn struct {
	mu       sync.RWMutex
	conn     net.Conn
	txStatus byte
	closed   bool
}

func NewPgConn(conn net.Conn) *PgConn {
	return &PgConn{
		conn:     conn,
		txStatus: TxStatusIdle,
	}
}

func (c *PgConn) TxStatus() byte {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.txStatus
}

func (c *PgConn) SetTxStatus(status byte) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.txStatus = status
}

func (c *PgConn) IsClosed() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.closed
}

func (c *PgConn) Exec(ctx context.Context, sql string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return ErrConnectionClosed
	}

	if err := ctx.Err(); err != nil {
		return err
	}

	switch sql {
	case "BEGIN":
		c.txStatus = TxStatusInTransaction
	case "COMMIT":
		c.txStatus = TxStatusIdle
	case "ROLLBACK":
		c.txStatus = TxStatusIdle
	}
	return nil
}

func (c *PgConn) Close(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return nil
	}
	c.closed = true
	c.txStatus = TxStatusIdle
	if c.conn != nil {
		_ = c.conn.SetDeadline(time.Now().Add(500 * time.Millisecond))
		return c.conn.Close()
	}
	return nil
}
