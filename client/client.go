package main

import (
	"fmt"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"log"
	"server.go/KVService"
)

func main() {
	// connect to grpc server
	ports := []string{"8028", "8029", "8030", "8031"}
	conn, err := grpc.Dial("192.168.67.2:8028", grpc.WithInsecure())
	if err != nil {
		log.Fatal("Fail to connect:", err)
	}
	defer conn.Close()

	// establish grpc client to get leaderID
	c := KVService.NewHandlerClient(conn)
	leaderID := KVService.LeaderID{}
	returnID, err := c.GetLeaderID(context.Background(), &leaderID)
	id := returnID.Id

	// connect to leader
	conn, err = grpc.Dial("192.168.67.2:" + ports[id], grpc.WithInsecure())
	if err != nil {
		log.Fatal("Fail to connect:", err)
	}
	c = KVService.NewHandlerClient(conn)
	fmt.Printf("Connect to Leader with ID = %d\n", returnID)

	// send requests
	key := []byte("hello")
	value := []byte("world")
	kv := KVService.KV{Key: key, Value: value}
	response, err := c.Put(context.Background(), &kv)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Put key-value (%s, %s)\n", response.ReturnKV.Key, response.ReturnKV.Value)

	response, err = c.Get(context.Background(), &kv)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Get key-value (%s, %s)\n", response.ReturnKV.Key, response.ReturnKV.Value)

	response, err = c.Delete(context.Background(), &kv)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Delete key-value (%s, %s)\n", response.ReturnKV.Key, response.ReturnKV.Value)
}
