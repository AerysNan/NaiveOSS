package main

import (
	"net/http"
	"oss/proxy"

	pm "oss/proto/metadata"

	"github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"gopkg.in/alecthomas/kingpin.v2"
)

var (
	address  = kingpin.Flag("address", "listen address of proxy server").Default("127.0.0.1:8082").String()
	metadata = kingpin.Flag("metadata", "listen address of metadata server").Default("127.0.0.1:8081").String()
	debug    = kingpin.Flag("debug", "use debug level of logging").Default("false").Bool()
)

func main() {
	kingpin.Parse()
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
	metadataClient := pm.NewMetadataForProxyClient(connection)
	server := proxy.NewProxyServer(*address, metadataClient)
	router := proxy.NewRouter(server)
	logrus.WithField("address", *address).Info("Server started")
	err = http.ListenAndServe(*address, router)
	if err != nil {
		logrus.WithError(err).Error("Server failed")
	}
}
