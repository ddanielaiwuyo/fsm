package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"net/rpc"
)

type Transport interface {
	Start(ctx context.Context, incoming chan<- RPC, outgoing <-chan RPC)
	Listen()
	Send()
}

type TestReq struct {
	From    string
	Message string
}

func (s *Server) TestServer(req TestReq, res *TestReq) error {
	reply := make(chan string)
	s.incoming <- RPC{payload: req.Message, reply: reply}

	msg := <-reply

	res.Message = msg
	res.From = "test-server"
	return nil
}

type Server struct {
	incoming chan RPC
	// todo: we might not need this afterall
	outgoing chan any
}

func NewServer(in chan RPC, out chan any) *Server {
	return &Server{
		incoming: in,
		outgoing: out,
	}
}

func (s *Server) Listen(ctx context.Context, addr string) error {
	handler := rpc.NewServer()
	if err := handler.Register(s); err != nil {
		return fmt.Errorf("could not start rpcServer. %w", err)
	}

	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("could not start server: %w", err)
	}

	go func() {
		<-ctx.Done()
		ln.Close()
	}()

	log.Println("tcp server active at", addr)
	for {
		conn, err := ln.Accept()
		if err != nil {
			if errors.Is(err, net.ErrClosed) {
				log.Println("listener closed")
				return nil
			}
			log.Println("could not accept connection. %w", err)
			continue
		}

		go handler.ServeConn(conn)

	}

}
