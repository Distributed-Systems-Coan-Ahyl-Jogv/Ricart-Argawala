package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"strings"
	"sync"
	"time"

	proto "ARGAWALAServer/grpc"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type Server struct {
	proto.UnimplementedArgaWalaServer

	id    string  
	addr  string  
	peers []string
	clis  map[string]proto.ArgaWalaClient

	mu         sync.Mutex
	clock      int64
	requesting bool
	reqTS      int64
	grants     int
	deferred   map[string]bool 
	inCS       bool
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



func (s *Server) SendRequest(ctx context.Context, req *proto.Request) (*proto.Empty, error) {
	sender := req.From

	s.mu.Lock()
	s.bump(req.Time)

	
	shouldDefer := s.requesting &&
		(s.reqTS < req.Time || (s.reqTS == req.Time && s.id < sender))

	if shouldDefer {
		s.deferred[sender] = true
		s.mu.Unlock()
		log.Printf("[%s] DEFER to %s (my ts=%d, their ts=%d)", s.id, sender, s.reqTS, req.Time)
		return &proto.Empty{}, nil
	}

	s.mu.Unlock()

	
	go s.sendGrant(sender)
	log.Printf("[%s] GRANT immediately to %s", s.id, sender)
	return &proto.Empty{}, nil
}


func (s *Server) SendReply(ctx context.Context, rep *proto.Reply) (*proto.Empty, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.grants++
	log.Printf("[%s] received GRANT (%d/%d) message=%q", s.id, s.grants, len(s.peers), rep.Message)
	return &proto.Empty{}, nil
}



func (s *Server) sendGrant(peerAddr string) {
	var cli proto.ArgaWalaClient
	var ok bool
	if cli, ok = s.clis[peerAddr]; !ok {
		conn, err := grpc.Dial(peerAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err == nil {
			cli = proto.NewArgaWalaClient(conn)
			s.clis[peerAddr] = cli
		}
	}

	backoff := 150 * time.Millisecond
	for attempt := 1; attempt <= 5; attempt++ {
		if cli == nil {

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



func (s *Server) requestCS() {
	s.broadcastRequest()


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
	wasInCS := s.inCS
	s.inCS = false
	wasRequesting := s.requesting
	s.requesting = false
	s.reqTS = 0
	s.grants = 0

	deferred := make([]string, 0, len(s.deferred))
	for p := range s.deferred {
		deferred = append(deferred, p)
	}
	s.deferred = make(map[string]bool)
	s.mu.Unlock()

	if wasInCS {
		log.Printf("[%s] <<< EXIT  CS", s.id)
	} else if wasRequesting {
		log.Printf("[%s] CANCEL request (leaving the queue)", s.id)
	} else {
		log.Printf("[%s] nothing to exit", s.id)
	}

	for _, p := range deferred {
		go s.sendGrant(p)
	}
	if len(deferred) > 0 {
		log.Printf("[%s] RELEASE; granted to %d deferred", s.id, len(deferred))
	}
}


func pickPortConfig() (string, string, []string) {
	configs := []string{":5001", ":5002", ":5003"} 


	var myPort string
	for _, port := range configs {
		ln, err := net.Listen("tcp", port)
		if err == nil {
			ln.Close()
			myPort = port
			break
		}
	}
	if myPort == "" {
		log.Fatalf("No free port found!")
	}


	var id string
	for i, p := range configs {
		if p == myPort {
			id = fmt.Sprintf("n%d", i+1)
			break
		}
	}


	peers := make([]string, 0)
	for _, p := range configs {
		if p != myPort {
			peers = append(peers, p)
		}
	}

	return id, myPort, peers
}


func main() {


	id, addr, peers := pickPortConfig()

	s := NewServer(id, addr, peers)


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


	time.Sleep(300 * time.Millisecond)
	s.connectPeers()

	log.Printf("[%s] type 'req'  + Enter to request the Critical Section", s.id)

	for {
		var cmd string
		fmt.Scanln(&cmd)
		switch strings.TrimSpace(cmd) {
		case "req":
			go s.requestCS()
		case "exit":
			if s.inCS {
				s.exitCS()
			} else {
				log.Printf("[%s] not in CS - nothing to exit", s.id)
			}
		default:
			log.Printf("[%s] unknown command: %q (try 'req' or 'exit')", s.id, cmd)
		}
	}
}
