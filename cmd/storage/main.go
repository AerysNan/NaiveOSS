package main

import (
	"flag"
	"net"
	pm "oss/proto/metadata"
	ps "oss/proto/storage"
	"oss/storage"

	"github.com/sirupsen/logrus"
	"google.golang.org/grpc"
)

var (
	address  = flag.String("address", "127.0.0.1:8080", "listen address of storage server")
	metadata = flag.String("metadata", "127.0.0.1:8081", "address of metadata server")
	root     = flag.String("root", "../data", "storage root of object file")
	debug    = flag.Bool("debug", false, "use debug level of loggin")
)

func main() {
	flag.Int64Var(&storage.VolumeMaxSize, "volume-size", 1<<10, "maximum size of volume")
	flag.Parse()
	if *debug {
		logrus.SetLevel(logrus.DebugLevel)
		logrus.Debug("Log level set to debug")
	}
	connection, err := grpc.Dial(*metadata, grpc.WithInsecure())
	if err != nil {
		logrus.WithError(err).Fatal("Connect to metadata server failed")
		return
	}
	defer connection.Close()
	metadataClient := pm.NewMetadataForStorageClient(connection)
	storageServer := storage.NewStorageServer(*address, *root, metadataClient)
	listen, err := net.Listen("tcp", *address)
	if err != nil {
		logrus.WithError(err).Fatal("Listen port failed")
	}
	server := grpc.NewServer()
	ps.RegisterStorageForMetadataServer(server, storageServer)
	ps.RegisterStorageForProxyServer(server, storageServer)
	logrus.WithField("address", *address).Info("Server started")
	if err = server.Serve(listen); err != nil {
		logrus.WithError(err).Fatal("Server failed")
	}
}
