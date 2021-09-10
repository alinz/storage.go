package sqlite

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"strings"

	"zombiezen.com/go/sqlite"
	"zombiezen.com/go/sqlite/sqlitex"

	"github.com/alinz/hash.go"
	"github.com/alinz/storage.go"
)

type Storage struct {
	buffer      *bytes.Buffer
	pool        *sqlitex.Pool
	maxDataSize int64
}

var _ storage.Putter = (*Storage)(nil)
var _ storage.Getter = (*Storage)(nil)
var _ storage.Remover = (*Storage)(nil)
var _ storage.Lister = (*Storage)(nil)
var _ storage.Closer = (*Storage)(nil)

func (s *Storage) put(ctx context.Context, conn *sqlite.Conn, r io.Reader) (hashValue []byte, n int64, err error) {
	defer sqlitex.Save(conn)(&err)

	s.buffer.Reset()
	n, err = io.Copy(s.buffer, r)
	if err != nil {
		return nil, n, err
	}

	stmt, err := conn.Prepare("INSERT INTO blobs (data) VALUES ($data);")
	if err != nil {
		return nil, 0, err
	}
	defer stmt.Finalize()

	stmt.SetZeroBlob("$data", n)

	if _, err := stmt.Step(); err != nil {
		return nil, 0, err
	}
	rowid := conn.LastInsertRowID()

	b, err := conn.OpenBlob("", "blobs", "data", rowid, true)
	if err != nil {
		return nil, 0, err
	}
	defer b.Close()

	hr := hash.NewReader(s.buffer)
	n, err = io.Copy(b, hr)
	if err != nil {
		return nil, n, err
	}

	hashValue = hr.Hash()

	updateHashValueStmt, err := conn.Prepare("UPDATE blobs SET hash_value = $hash_value WHERE rowid = $rowid;")
	if err != nil {
		return nil, n, err
	}
	defer updateHashValueStmt.Finalize()

	updateHashValueStmt.SetText("$hash_value", hash.Format(hashValue))
	updateHashValueStmt.SetInt64("$rowid", rowid)

	if _, err := updateHashValueStmt.Step(); err != nil {
		return nil, n, err
	}

	return hashValue, n, nil
}

func (s *Storage) Put(ctx context.Context, r io.Reader) ([]byte, int64, error) {
	conn, closeConn, err := s.conn(ctx)
	if err != nil {
		return nil, 0, err
	}
	defer closeConn()

	return s.put(ctx, conn, r)
}

func (s *Storage) Get(ctx context.Context, hashValue []byte) (io.ReadCloser, error) {
	conn, closeConn, err := s.conn(ctx)
	if err != nil {
		return nil, err
	}

	stmt, err := conn.Prepare("SELECT rowid, data FROM blobs WHERE hash_value = $hash_value;")
	if err != nil {
		closeConn()
		return nil, err
	}
	defer stmt.Finalize()

	stmt.SetText("$hash_value", hash.Format(hashValue))

	rowReturned, err := stmt.Step()
	if err != nil {
		closeConn()
		return nil, err
	}

	if !rowReturned {
		closeConn()
		return nil, storage.ErrNotFound
	}

	rowid := stmt.GetInt64("rowid")

	b, err := conn.OpenBlob("", "blobs", "data", rowid, false)
	if err != nil {
		closeConn()
		return nil, err
	}

	return &customReadCloser{rc: b, closeConn: closeConn}, nil
}

func (s *Storage) remove(conn *sqlite.Conn, hashValue []byte) (err error) {
	defer sqlitex.Save(conn)(&err)

	stmt, err := conn.Prepare("DELETE FROM blobs WHERE hash_value = $hash_value;")
	if err != nil {
		return err
	}
	defer stmt.Finalize()

	stmt.SetText("$hash_value", hash.Format(hashValue))

	_, err = stmt.Step()
	if err != nil {
		return err
	}

	return nil
}

func (s *Storage) Remove(ctx context.Context, hashValue []byte) error {
	conn, closeConn, err := s.conn(ctx)
	if err != nil {
		return err
	}
	defer closeConn()

	return s.remove(conn, hashValue)
}

func (s *Storage) List() (storage.IteratorFunc, storage.CancelFunc) {
	mapper := func(yield storage.YieldFunc) {
		ctx := context.Background()
		conn, closeConn, err := s.conn(ctx)
		if err != nil {
			yield(nil, err)
			return
		}
		defer closeConn()

		stmt, err := conn.Prepare("SELECT hash_value FROM blobs;")
		if err != nil {
			yield(nil, err)
			return
		}
		defer stmt.Finalize()

		for {
			rowReturned, err := stmt.Step()
			if err != nil {
				yield(nil, err)
				return
			}

			if !rowReturned {
				yield(nil, storage.ErrIteratorDone)
				break
			}

			value := stmt.GetText("hash_value")
			hashValue, err := hash.ValueFromString(value)
			if err != nil {
				yield(nil, err)
				return
			}

			yield(hashValue, nil)
		}
	}

	return storage.Iterator(mapper)
}

func (s *Storage) conn(ctx context.Context) (*sqlite.Conn, func(), error) {
	conn := s.pool.Get(ctx)
	if conn == nil {
		return nil, nil, context.Canceled
	}

	return conn, func() { s.pool.Put(conn) }, nil
}

func (s *Storage) createTable() (err error) {

	conn, close, err := s.conn(context.Background())
	if err != nil {
		return err
	}
	defer close()

	sql := strings.TrimSpace(`
		CREATE TABLE IF NOT EXISTS blobs (
			hash_value TEXT, 
			data blob
		);

		CREATE INDEX IF NOT EXISTS blobs_hash_value ON blobs (hash_value);
	`)

	return sqlitex.ExecScript(conn, sql)
}

func (s *Storage) Close() error {
	return s.pool.Close()
}

func New(stringConn string, poolSize int, maxDataSize int64) (*Storage, error) {
	pool, err := sqlitex.Open(stringConn, 0, poolSize)
	if err != nil {
		return nil, err
	}

	s := &Storage{
		pool:        pool,
		maxDataSize: maxDataSize,
		buffer:      bytes.NewBuffer(make([]byte, 0, maxDataSize)),
	}

	err = s.createTable()
	if err != nil {
		return nil, err
	}

	return s, nil
}

func NewFile(dbPath string, poolSize int, maxDataSize int64) (*Storage, error) {
	stringConn := fmt.Sprintf("file:%s", dbPath)
	return New(stringConn, poolSize, maxDataSize)
}

func NewMemory(poolSize int, maxDataSize int64) (*Storage, error) {
	return New("file::memory:?cache=shared", poolSize, maxDataSize)
}

type customReadCloser struct {
	closeConn func()
	rc        io.ReadCloser
}

func (c *customReadCloser) Read(p []byte) (int, error) {
	return c.rc.Read(p)
}

func (c *customReadCloser) Close() error {
	defer c.closeConn()

	err := c.rc.Close()
	if err != nil {
		return err
	}
	return nil
}
