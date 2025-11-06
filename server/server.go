package main

import (
	proto "ARGAWALAServer/grpc"
	"bufio"
	"fmt"
	"net"
	"os"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
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

	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan() {
		text := scanner.Text()
		if text == "" || len(text) > 128 {
			continue
		}
		conn, err := grpc.Dial(text, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			fmt.Println("Failed to connect:", err)
			continue
		}
		defer conn.Close()
	}
}
