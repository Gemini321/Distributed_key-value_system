package main

import (
	"fmt"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/store/tikv"
	"github.com/prometheus/common/log"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
	"net"
	"server.go/KVService"
	"server.go/server"
)

func main() {
	// listen port and establish grpc service
	lis, err := net.Listen("tcp", ":8031")
	if err != nil {
		log.Fatal("Fail to listen port:", err)
	}
	s := grpc.NewServer()

	// establish connection with TiKV server
	cli, err := tikv.NewRawKVClient([]string{"192.168.67.131:2379"}, config.Security{})
	if err != nil {
		panic(err)
	}
	defer cli.Close()
	fmt.Printf("cluster ID: %d\n", cli.ClusterID())

	// register grpc interface
	setting := server.ServerSetting{}
	setting.LocalIP = "192.168.67.2"
	setting.LocalPort = "8031"
	setting.Sid = 3
	for i := 0; i < server.PeerNum; i ++ {
		setting.PeerIP = append(setting.PeerIP, "192.168.67.2")
	}
	setting.PeerPorts = append(setting.PeerPorts, "8028")
	setting.PeerPorts = append(setting.PeerPorts, "8029")
	setting.PeerPorts = append(setting.PeerPorts, "8030")
	tmpServer := server.Server{}
	setting.Cli = cli
	for i := 0; i < server.PeerNum; i ++ {
		// connect to peer grpc server
		conn, err := grpc.Dial(setting.PeerIP[i] + ":" + setting.PeerPorts[i], grpc.WithInsecure())
		if err != nil {
			log.Fatal("Fail to connect:", err)
		}

		// create peer grpc client
		c := KVService.NewHandlerClient(conn)
		setting.PeerHandlers = append(setting.PeerHandlers, c)
	}
	tmpServer.Set(setting)
	KVService.RegisterHandlerServer(s, &tmpServer)
	fmt.Println("Successfully connect to all servers")

	// register reflection on grpc service
	reflection.Register(s)
	fmt.Println("Successfully register server")

	// begin election
	tmpServer.Start()

	// grant listen privilege to grpc service
	err = s.Serve(lis)
	if err != nil {
		log.Fatal("Fail to serve:", err)
	}
}