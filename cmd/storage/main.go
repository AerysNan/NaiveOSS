package main

import (
	"encoding/json"
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
	metadata = kingpin.Flag("metadata", "listen address of metadata server").Default("0.0.0.0:8081").String()
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
	storageServer := storage.NewStorageServer(metadataClient, config)
	listen, err := net.Listen("tcp", config.Address)
	if err != nil {
		logrus.WithError(err).Fatal("Listen port failed")
	}
	server := grpc.NewServer(grpc.MaxRecvMsgSize(global.MaxTransportSize), grpc.MaxSendMsgSize(global.MaxTransportSize))
	ps.RegisterStorageForMetadataServer(server, storageServer)
	ps.RegisterStorageForProxyServer(server, storageServer)
	logrus.WithField("address", config.Address).Infof("Server started with data directory %v", config.Root)
	if err = server.Serve(listen); err != nil {
		logrus.WithError(err).Fatal("Server failed")
	}
}
