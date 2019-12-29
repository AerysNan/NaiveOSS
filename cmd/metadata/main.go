package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net"
	"os"
	"oss/metadata"
	pm "oss/proto/metadata"

	"github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"gopkg.in/alecthomas/kingpin.v2"
)

var (
	address = kingpin.Flag("address", "address of meta server").Default("0.0.0.0:8081").String()
	port    = kingpin.Flag("port", "listen port of meta server").Default("8081").String()
	root    = kingpin.Flag("root", "metadata file path").Default("../data").String()
	config  = kingpin.Flag("config", "config file full name").Default("../config/metadata.json").String()
	debug   = kingpin.Flag("debug", "use debug level of logging").Default("false").Bool()
)

func main() {
	kingpin.Parse()
	if *debug {
		logrus.SetLevel(logrus.DebugLevel)
		logrus.Debug("Log level set to debug")
	}
	file, err := os.Open(*config)
	if err != nil {
		logrus.WithError(err).Fatal("Open config file failed")
	}
	config := new(metadata.Config)
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
	metadataServer := metadata.NewMetadataServer(*address, *root, config)
	listenAddress := fmt.Sprintf("%s:%s", "0.0.0.0", *port)
	listen, err := net.Listen("tcp", listenAddress)
	if err != nil {
		logrus.WithError(err).Fatal("Listen port failed")
	}
	server := grpc.NewServer()
	pm.RegisterMetadataForStorageServer(server, metadataServer)
	pm.RegisterMetadataForProxyServer(server, metadataServer)
	logrus.WithField("address", listenAddress).Info("Server started")
	if err = server.Serve(listen); err != nil {
		logrus.WithError(err).Fatal("Server failed")
	}
}
