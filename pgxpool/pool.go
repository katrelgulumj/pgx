package pgxpool

import (
	"context"
	"errors"
	"sync"
	"time"

	"katrelgulumj/pgx"
	"katrelgulumj/pgx/pgconn"
)

var (
	ErrPoolClosed = errors.New("connection pool is closed")
)

type Stat struct {
	AcquiredConns  int32
	IdleConns      int32
	TotalConns     int32
	DestroyedConns int64
}

type Pool struct {
	mu             sync.Mutex
	idleConns      []*pgx.Conn
	acquiredConns  map[*pgx.Conn]struct{}
	destroyedCount int64
	closed         bool
}

func NewPool() *Pool {
	return &Pool{
		idleConns:     make([]*pgx.Conn, 0),
		acquiredConns: make(map[*pgx.Conn]struct{}),
	}
}

func (p *Pool) Acquire(ctx context.Context) (*Conn, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.closed {
		return nil, ErrPoolClosed
	}

	for len(p.idleConns) > 0 {
		conn := p.idleConns[len(p.idleConns)-1]
		p.idleConns = p.idleConns[:len(p.idleConns)-1]

		if conn.IsClosed() || conn.PgConn().TxStatus() != pgconn.TxStatusIdle {
			p.destroyedCount++
			_ = conn.Close(ctx)
			continue
		}

		p.acquiredConns[conn] = struct{}{}
		return &Conn{pool: p, conn: conn}, nil
	}

	rawConn := pgx.NewConn(pgconn.NewPgConn(nil))
	p.acquiredConns[rawConn] = struct{}{}
	return &Conn{pool: p, conn: rawConn}, nil
}

func (p *Pool) Begin(ctx context.Context) (pgx.Tx, *Conn, error) {
	c, err := p.Acquire(ctx)
	if err != nil {
		return nil, nil, err
	}
	tx, err := c.Conn().Begin(ctx)
	if err != nil {
		c.Release()
		return nil, nil, err
	}
	return tx, c, nil
}

func (p *Pool) releaseConn(conn *pgx.Conn) {
	p.mu.Lock()
	defer p.mu.Unlock()

	delete(p.acquiredConns, conn)
	if p.closed {
		p.destroyedCount++
		_ = conn.Close(context.Background())
		return
	}
	p.idleConns = append(p.idleConns, conn)
}

func (p *Pool) destroyConn(conn *pgx.Conn) {
	p.mu.Lock()
	defer p.mu.Unlock()

	delete(p.acquiredConns, conn)
	p.destroyedCount++
	cleanupCtx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	_ = conn.Close(cleanupCtx)
}

func (p *Pool) Stat() Stat {
	p.mu.Lock()
	defer p.mu.Unlock()
	return Stat{
		AcquiredConns:  int32(len(p.acquiredConns)),
		IdleConns:      int32(len(p.idleConns)),
		TotalConns:     int32(len(p.acquiredConns) + len(p.idleConns)),
		DestroyedConns: p.destroyedCount,
	}
}

func (p *Pool) Close() {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.closed {
		return
	}
	p.closed = true

	for _, conn := range p.idleConns {
		p.destroyedCount++
		_ = conn.Close(context.Background())
	}
	p.idleConns = nil

	for conn := range p.acquiredConns {
		p.destroyedCount++
		_ = conn.Close(context.Background())
	}
	p.acquiredConns = make(map[*pgx.Conn]struct{})
}
