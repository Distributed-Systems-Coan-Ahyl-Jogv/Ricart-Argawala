package main

import (
	proto "ARGAWALAServer/grpc"
	"fmt"
	"net"
	"os"

	"google.golang.org/grpc"
)

type ArgawalaServer struct {
	proto.UnimplementedArgaWalaServer
}

func main() {
	port := os.Args[1]
	fmt.Print(port)
	server := grpc.NewServer()
	listener, err := net.Listen("tcp", port)
	if err != nil {
		panic(err)
	}
	proto.RegisterArgaWalaServer(server, &ArgawalaServer{})
	err = server.Serve(listener)
	if err != nil {
		panic(err)
	}
}
