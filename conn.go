package pgx

import (
	"context"
	"katrelgulumj/pgx/pgconn"
)

type Conn struct {
	pgConn *pgconn.PgConn
}

func NewConn(pgConn *pgconn.PgConn) *Conn {
	return &Conn{pgConn: pgConn}
}

func (c *Conn) PgConn() *pgconn.PgConn {
	return c.pgConn
}

func (c *Conn) Exec(ctx context.Context, sql string) error {
	return c.pgConn.Exec(ctx, sql)
}

func (c *Conn) Close(ctx context.Context) error {
	return c.pgConn.Close(ctx)
}

func (c *Conn) IsClosed() bool {
	return c.pgConn.IsClosed()
}

func (c *Conn) Begin(ctx context.Context) (Tx, error) {
	return Begin(ctx, c)
}
