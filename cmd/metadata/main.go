package main

import (
	"flag"
	"net"
	"oss/metadata"
	pm "oss/proto/metadata"

	"github.com/sirupsen/logrus"
	"google.golang.org/grpc"
)

var (
	address = flag.String("address", "127.0.0.1:8081", "listen address of metadata server")
	debug   = flag.Bool("debug", false, "use debug level of loggin")
)

func main() {
	flag.Parse()
	if *debug {
		logrus.SetLevel(logrus.DebugLevel)
		logrus.Debug("Log level set to debug")
	}
	metadataServer := metadata.NewMetadataServer(*address)
	listen, err := net.Listen("tcp", *address)
	if err != nil {
		logrus.WithError(err).Fatal("Listen port failed")
	}
	server := grpc.NewServer()
	pm.RegisterMetadataForStorageServer(server, metadataServer)
	pm.RegisterMetadataForProxyServer(server, metadataServer)
	logrus.WithField("address", *address).Info("Server started")
	if err = server.Serve(listen); err != nil {
		logrus.WithError(err).Fatal("Server failed")
	}
}
