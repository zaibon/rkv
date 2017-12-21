package redis

import (
	"bufio"
	"encoding/hex"
	"fmt"
	"net"
	"net/textproto"
	"strconv"
	"strings"

	"github.com/zaibon/rkv/storage"

	log "github.com/sirupsen/logrus"
)

type connection struct {
	conn net.Conn
	r    *textproto.Reader
	w    *textproto.Writer
	br   *bufio.Reader

	storage *storage.Storage
}

func newConnection(conn net.Conn, storage *storage.Storage) *connection {
	// create proper reader
	// to avoid denial of service attacks, use LimiteReader
	// lr := io.LimitReader(conn, 8192)
	// then user bufio to limit nubmer of system call
	br := bufio.NewReaderSize(conn, 2*1024*1024)
	tpr := textproto.NewReader(br)

	bw := bufio.NewWriter(conn)
	tpw := textproto.NewWriter(bw)

	return &connection{
		conn:    conn,
		br:      br,
		r:       tpr,
		w:       tpw,
		storage: storage,
	}
}

func (c *connection) Close() {
	c.conn.Close()
}

func (c *connection) handle() error {
	defer c.conn.Close()

	for {

		line, err := c.r.ReadLine()
		if err != nil {
			return err
		}
		if line[:1] != "*" {
			return fmt.Errorf("not an array")
		}

		argc, err := strconv.Atoi(line[1:2])
		if err != nil {
			return err
		}

		req := &request{
			args: make([][]byte, argc),
		}

		for i := 0; i < argc; i++ {
			line, err := c.r.ReadLine()
			if err != nil {
				return err
			}

			if line[:1] != "$" {
				return fmt.Errorf("Malformed request (string)")
			}
			size, err := strconv.Atoi(line[1:])
			if err != nil {
				return err
			}

			buf := make([]byte, size+2)
			_, err = c.r.R.Read(buf)
			if err != nil {
				return err
			}
			req.args[i] = buf[:len(buf)-2]
		}

		if err := c.dispatcher(req); err != nil {
			log.Errorln(err)
			return err
		}
	}
}

func (c *connection) dispatcher(req *request) error {
	switch strings.ToLower(req.Command()) {
	case "ping":
		c.w.PrintfLine("+PONG")
	case "set":
		if len(req.args) < 3 {
			return fmt.Errorf("mal formatted request")
		}

		value := []byte(req.args[2])

		log.Debugln("trying to insert entry")
		hash, err := c.storage.Set(value)
		if err != nil {
			return err
		}

		c.w.PrintfLine("+%x", hash)

	case "get":
		if len(req.args) < 2 {
			return fmt.Errorf("mal formatted request, missing key")
		}
		key := req.args[1]
		if len(key) != 64 {
			log.Error("invalid key size")
			c.w.PrintfLine("-Invalid key")
			return nil
		}

		log.Debugln("trying to get entry")
		hash := make([]byte, 32)
		_, err := hex.Decode(hash, key)
		if err != nil {
			return err
		}

		data, err := c.storage.Get(hash)
		if err != nil {
			return err
		}

		c.w.PrintfLine("$%d", len(data))
		c.w.PrintfLine("%s", string(data))

	case "stop":
		c.w.PrintfLine("+Stopping")

	default:
		c.w.PrintfLine("-Command '%s' not handled", req.Command())
	}
	return nil
}
