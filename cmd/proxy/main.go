package main

import (
	"net/http"
	"oss/proxy"

	pa "oss/proto/auth"
	pm "oss/proto/metadata"

	"github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"gopkg.in/alecthomas/kingpin.v2"
)

var (
	address = kingpin.Flag("address", "listen address of proxy server").Default("127.0.0.1:8082").String()
	meta    = kingpin.Flag("meta", "listen address of meta server").Default("127.0.0.1:8081").String()
	auth    = kingpin.Flag("auth", "listen address of auth server").Default("127.0.0.1:8083").String()
	debug   = kingpin.Flag("debug", "use debug level of logging").Default("false").Bool()
)

func main() {
	kingpin.Parse()
	if *debug {
		logrus.SetLevel(logrus.DebugLevel)
		logrus.Debug("Log level set to debug")
	}
	metaConnection, err := grpc.Dial(*meta, grpc.WithInsecure())
	if err != nil {
		logrus.WithError(err).Fatal("Connect to metadata server failed")
		return
	}
	defer metaConnection.Close()
	metaClient := pm.NewMetadataForProxyClient(metaConnection)
	authConnection, err := grpc.Dial(*auth, grpc.WithInsecure())
	if err != nil {
		logrus.WithError(err).Fatal("Connect to metadata server failed")
		return
	}
	defer authConnection.Close()
	authClient := pa.NewAuthForProxyClient(authConnection)
	proxyServer := proxy.NewProxyServer(*address, authClient, metaClient)
	router := proxy.NewRouter(proxyServer)
	logrus.WithField("address", *address).Info("Server started")
	err = http.ListenAndServe(*address, router)
	if err != nil {
		logrus.WithError(err).Error("Server failed")
	}
}
