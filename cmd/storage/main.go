package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net"
	"os"
	"oss/global"
	pm "oss/proto/metadata"
	ps "oss/proto/storage"
	"oss/storage"

	"github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"gopkg.in/alecthomas/kingpin.v2"
)

var (
	address  = kingpin.Flag("address", "address of storage server").Default("0.0.0.0:8080").String()
	port     = kingpin.Flag("port", "listen port of storage server").Default("8080").String()
	metadata = kingpin.Flag("metadata", "listen address of metadata server").Default("0.0.0.0:8081").String()
	root     = kingpin.Flag("root", "metadata file path").Default("../data").String()
	config   = kingpin.Flag("config", "config file full name").Default("../config/storage.json").String()
	debug    = kingpin.Flag("debug", "use debug level of logging").Default("false").Bool()
)

func main() {
	kingpin.Parse()
	if *debug {
		logrus.SetLevel(logrus.DebugLevel)
		logrus.Debug("Log level set to debug")
	}
	metaConnection, err := grpc.Dial(*metadata, grpc.WithInsecure())
	if err != nil {
		logrus.WithError(err).Fatal("Connect to metadata server failed")
		return
	}
	defer metaConnection.Close()
	metadataClient := pm.NewMetadataForStorageClient(metaConnection)
	file, err := os.Open(*config)
	if err != nil {
		logrus.WithError(err).Fatal("Open config file failed")
	}
	config := new(storage.Config)
	bytes, err := ioutil.ReadAll(file)
	if err != nil {
		file.Close()
		logrus.WithError(err).Fatal("Read config file failed")
	}
	err = json.Unmarshal(bytes, config)
	if err != nil {
		file.Close()
		logrus.WithError(err).Fatal("Unmarshal JSON failed")
	}
	file.Close()
	storageServer := storage.NewStorageServer(*address, *root, metadataClient, config)
	listenAddress := fmt.Sprintf("%s:%s", "0.0.0.0", *port)
	listen, err := net.Listen("tcp", listenAddress)
	if err != nil {
		logrus.WithError(err).Fatal("Listen port failed")
	}
	server := grpc.NewServer(grpc.MaxRecvMsgSize(global.MaxTransportSize), grpc.MaxSendMsgSize(global.MaxTransportSize))
	ps.RegisterStorageForMetadataServer(server, storageServer)
	ps.RegisterStorageForProxyServer(server, storageServer)
	logrus.WithField("address", listenAddress).Info("Server started")
	if err = server.Serve(listen); err != nil {
		logrus.WithError(err).Fatal("Server failed")
	}
}
