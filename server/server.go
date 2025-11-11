package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"os"
	"strings"
	"sync"
	"time"

	proto "ARGAWALAServer/grpc"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type Server struct {
	proto.UnimplementedArgaWalaServer

	// identity & peers
	id    string   // human-friendly id, e.g. "n1"
	addr  string   // our listen addr, e.g. ":5001"
	peers []string // other nodes' addrs (host:port)
	clis  map[string]proto.ArgaWalaClient

	// RA state
	mu         sync.Mutex
	clock      int64
	requesting bool
	reqTS      int64
	grants     int
	deferred   map[string]bool // addr -> deferred?
	inCS bool
}

func NewServer(id, addr string, peers []string) *Server {
	return &Server{
		id:       id,
		addr:     addr,
		peers:    peers,
		clis:     make(map[string]proto.ArgaWalaClient),
		deferred: make(map[string]bool),
	}
}

func (s *Server) bump(incoming int64) int64 {
	if incoming > s.clock {
		s.clock = incoming
	}
	s.clock++
	return s.clock
}

func (s *Server) connectPeers() {
	for _, p := range s.peers {
		conn, err := grpc.Dial(p, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			log.Fatalf("[%s] dial %s: %v", s.id, p, err)
		}
		s.clis[p] = proto.NewArgaWalaClient(conn)
	}
}

/***************  gRPC handlers  ****************/

// Another node asks us for permission
func (s *Server) SendRequest(ctx context.Context, req *proto.Request) (*proto.Empty, error) {
	sender := req.From // use the sender’s listening address, not their client port

	s.mu.Lock()
	s.bump(req.Time)

	// Determine whether to defer or grant
	shouldDefer := s.requesting &&
		(s.reqTS < req.Time || (s.reqTS == req.Time && s.id < sender))

	if shouldDefer {
		s.deferred[sender] = true
		s.mu.Unlock()
		log.Printf("[%s] DEFER to %s (my ts=%d, their ts=%d)", s.id, sender, s.reqTS, req.Time)
		return &proto.Empty{}, nil
	}

	s.mu.Unlock()

	// Grant immediately if not deferring
	go s.sendGrant(sender)
	log.Printf("[%s] GRANT immediately to %s", s.id, sender)
	return &proto.Empty{}, nil
}

// We receive a grant
func (s *Server) SendReply(ctx context.Context, rep *proto.Reply) (*proto.Empty, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.grants++
	log.Printf("[%s] received GRANT (%d/%d) message=%q", s.id, s.grants, len(s.peers), rep.Message)
	return &proto.Empty{}, nil
}

/***************  client helpers  ****************/

func (s *Server) sendGrant(peerAddr string) {
    var cli proto.ArgaWalaClient
    var ok bool
    if cli, ok = s.clis[peerAddr]; !ok {
        // create on first use
        conn, err := grpc.Dial(peerAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
        if err == nil {
            cli = proto.NewArgaWalaClient(conn)
            s.clis[peerAddr] = cli
        }
    }

    backoff := 150 * time.Millisecond
    for attempt := 1; attempt <= 5; attempt++ {
        if cli == nil {
            // try to (re)dial
            conn, err := grpc.Dial(peerAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
            if err != nil {
                log.Printf("[%s] grant: dial %s (attempt %d): %v", s.id, peerAddr, attempt, err)
                time.Sleep(backoff)
                backoff *= 2
                continue
            }
            cli = proto.NewArgaWalaClient(conn)
            s.clis[peerAddr] = cli
        }

        ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
        _, err := cli.SendReply(ctx, &proto.Reply{Message: "grant"})
        cancel()
        if err == nil {
            return
        }
        log.Printf("[%s] SendReply to %s (attempt %d) failed: %v", s.id, peerAddr, attempt, err)
        time.Sleep(backoff)
        backoff *= 2
    }
}

func (s *Server) broadcastRequest() {
	s.mu.Lock()
	s.requesting = true
	ts := s.bump(s.clock)
	s.reqTS = ts
	s.grants = 0
	s.mu.Unlock()

	req := &proto.Request{Time: ts, From: s.addr}
	for peerAddr, cli := range s.clis {
		go func(p string, c proto.ArgaWalaClient) {
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			_, err := c.SendRequest(ctx, req)
			if err != nil {
				log.Printf("[%s] SendRequest to %s failed: %v", s.id, p, err)
			} else {
				log.Printf("[%s] sent REQUEST(ts=%d) to %s", s.id, ts, p)
			}
		}(peerAddr, cli)
	}
}

func (s *Server) release() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.requesting = false
	for p := range s.deferred {
		go s.sendGrant(p)
	}
	log.Printf("[%s] RELEASE; granted to %d deferred", s.id, len(s.deferred))
	s.deferred = make(map[string]bool)
}

/***************  CS driver  ****************/

func (s *Server) requestCS() {
    s.broadcastRequest()

    // Wait for all grants
    for {
        time.Sleep(50 * time.Millisecond)
        s.mu.Lock()
        done := (s.grants == len(s.peers))
        s.mu.Unlock()
        if done {
            break
        }
    }

    s.mu.Lock()
    s.inCS = true
    s.mu.Unlock()
    log.Printf("[%s] >>> ENTER CS (type 'exit' to leave)", s.id)
}
func (s *Server) exitCS() {
    s.mu.Lock()
    if !s.inCS && !s.requesting {
        s.mu.Unlock()
        log.Printf("[%s] not in CS and no pending request; ignoring 'exit'", s.id)
        return
    }
    wasInCS := s.inCS
    s.inCS = false
    s.requesting = false
    deferred := make([]string, 0, len(s.deferred))
    for p := range s.deferred {
        deferred = append(deferred, p)
    }
    s.deferred = make(map[string]bool)
    s.mu.Unlock()

    if wasInCS {
        log.Printf("[%s] <<< EXIT  CS", s.id)
    } else {
        log.Printf("[%s] CANCEL request (not entering CS)", s.id)
    }
    for _, p := range deferred {
        go s.sendGrant(p)
    }
    log.Printf("[%s] RELEASE; granted to %d deferred", s.id, len(deferred))
}
/***************  main  ****************/

func main() {
	// Usage:
	// go run ./server --id=n1 --addr=:5001 --peers=:5002,:5003
	if len(os.Args) < 4 {
		fmt.Println("Usage: server --id=<id> --addr=<host:port> --peers=<h:p,h:p,...>")
		os.Exit(1)
	}
	id := strings.SplitN(os.Args[1], "=", 2)[1]
	addr := strings.SplitN(os.Args[2], "=", 2)[1]
	peerCSV := strings.SplitN(os.Args[3], "=", 2)[1]
	var peers []string
	if peerCSV != "" {
		peers = strings.Split(peerCSV, ",")
	}

	s := NewServer(id, addr, peers)

	// Start gRPC server
	gs := grpc.NewServer()
	proto.RegisterArgaWalaServer(gs, s)
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		log.Fatalf("listen %s: %v", addr, err)
	}
	go func() {
		log.Printf("[%s] listening on %s; peers=%v", s.id, s.addr, s.peers)
		if err := gs.Serve(lis); err != nil {
			log.Fatal(err)
		}
	}()

	// Connect to peers (give them a moment to start)
	time.Sleep(300 * time.Millisecond)
	s.connectPeers()

	log.Printf("[%s] type 'req'  + Enter to request the Critical Section", s.id)
	log.Printf("[%s] type 'exit' + Enter to leave the Critical Section (or cancel if pending)", s.id)
	
	for {
		var cmd string
		fmt.Scanln(&cmd)
		switch strings.TrimSpace(cmd) {
		case "req":
			go s.requestCS()
		case "exit":
			s.exitCS()
		default:
			log.Printf("[%s] unknown command: %q (try 'req' or 'exit')", s.id, cmd)
		}
	}}
