package redis

import (
	"io"
	"net"

	log "github.com/sirupsen/logrus"
	"github.com/zaibon/rkv/storage"
)

// NewServer creates a new redis compatible server
func NewServer(storage *storage.Storage) *Server {
	return &Server{
		storage: storage,
	}
}

// Server is the basic struct that holds to listen
// and server request from redis client
type Server struct {
	storage *storage.Storage
	l       net.Listener
}

// Listen opens a listening tcp socket on addr and start server
// client requests
func (s *Server) Listen(addr string) error {
	l, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	defer l.Close()

	log.Infof("Listen on %s", l.Addr().String())
	cConn := make(chan net.Conn)

	go func() {
		defer close(cConn)
		for {
			plainCon, err := l.Accept()
			if err != nil {
				log.Errorln(err)
				return
			}
			cConn <- plainCon
		}
	}()

	for plainCon := range cConn {
		log.Infof("connection from %s", plainCon.RemoteAddr())
		go func(plainCon net.Conn) {

			conn := newConnection(plainCon, s.storage)
			defer func() {
				conn.Close()
				log.Infof("connection from %s closed", plainCon.RemoteAddr())
			}()

			if err = conn.handle(); err != nil && err != io.EOF {
				log.Errorf("client %s error: %v", plainCon.RemoteAddr(), err)
				return
			}

		}(plainCon)
	}
	return nil
}

// Close stops receiving requests and stops the server
func (s *Server) Close() error {
	if s.l != nil {
		return s.l.Close()
	}
	return nil
}
