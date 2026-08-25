package pgxpool

import (
	"context"
	"sync"
	"time"

	"katrelgulumj/pgx"
	"katrelgulumj/pgx/pgconn"
)

type Conn struct {
	pool   *Pool
	conn   *pgx.Conn
	released bool
	mu     sync.Mutex
}

func (c *Conn) Conn() *pgx.Conn {
	return c.conn
}

func (c *Conn) Release() {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.released {
		return
	}
	c.released = true

	if c.conn.IsClosed() {
		c.pool.destroyConn(c.conn)
		return
	}

	txStatus := c.conn.PgConn().TxStatus()
	if txStatus != pgconn.TxStatusIdle {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
		err := c.conn.Exec(cleanupCtx, "ROLLBACK")
		cancel()

		if err != nil || c.conn.PgConn().TxStatus() != pgconn.TxStatusIdle {
			c.pool.destroyConn(c.conn)
			return
		}
	}

	c.pool.releaseConn(c.conn)
}

func (c *Conn) Destroy() {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.released {
		return
	}
	c.released = true
	c.pool.destroyConn(c.conn)
}
