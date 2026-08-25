package pgx

import (
	"context"
	"errors"
	"sync"
	"time"

	"katrelgulumj/pgx/pgconn"
)

var (
	ErrTxClosed = errors.New("tx is closed")
)

type Tx interface {
	Commit(ctx context.Context) error
	Rollback(ctx context.Context) error
	Conn() *Conn
}

type tx struct {
	conn   *Conn
	mu     sync.Mutex
	closed bool
}

func Begin(ctx context.Context, conn *Conn) (Tx, error) {
	err := conn.Exec(ctx, "BEGIN")
	if err != nil {
		return nil, err
	}
	return &tx{conn: conn}, nil
}

func (t *tx) Conn() *Conn {
	return t.conn
}

func (t *tx) Commit(ctx context.Context) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.closed {
		return ErrTxClosed
	}
	t.closed = true

	err := t.conn.Exec(ctx, "COMMIT")
	if err != nil {
		t.conn.PgConn().SetTxStatus(pgconn.TxStatusInError)
		return err
	}
	return nil
}

func (t *tx) Rollback(ctx context.Context) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.closed {
		return ErrTxClosed
	}
	t.closed = true

	if t.conn.PgConn().TxStatus() == pgconn.TxStatusIdle {
		return nil
	}

	if ctx.Err() != nil {
		fallbackCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		err := t.conn.Exec(fallbackCtx, "ROLLBACK")
		if err != nil {
			_ = t.conn.Close(fallbackCtx)
			return err
		}
		return nil
	}

	err := t.conn.Exec(ctx, "ROLLBACK")
	if err != nil {
		fallbackCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = t.conn.Exec(fallbackCtx, "ROLLBACK")
		if t.conn.PgConn().TxStatus() != pgconn.TxStatusIdle {
			_ = t.conn.Close(fallbackCtx)
		}
		return err
	}
	return nil
}
