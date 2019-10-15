package main

import (
	"flag"
	"net/http"
	"oss/proxy"

	pm "oss/proto/metadata"

	"github.com/sirupsen/logrus"
	"google.golang.org/grpc"
)

var (
	address  = flag.String("address", "127.0.0.1:8082", "listen address of proxy server")
	metadata = flag.String("metadata", "127.0.0.1:8081", "listen address of metadata server")
	debug    = flag.Bool("debug", false, "use debug level of loggin")
)

func main() {
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
	metadataClient := pm.NewMetadataForProxyClient(connection)
	server := proxy.NewProxyServer(*address, metadataClient)
	logrus.WithField("address", *address).Info("Server started")
	err = http.ListenAndServe(*address, server)
	if err != nil {
		logrus.WithError(err).Error("Server failed")
	}
}
