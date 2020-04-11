package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net"
	"os"
	"oss/auth"
	pa "oss/proto/auth"

	"github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"gopkg.in/alecthomas/kingpin.v2"
)

var (
	port   = kingpin.Flag("port", "listen port of auth server").Default("8083").String()
	root   = kingpin.Flag("root", "database file path").Default("../data/auth/").String()
	config = kingpin.Flag("config", "config file full name").Default("../config/auth.json").String()
	debug  = kingpin.Flag("debug", "use debug level of logging").Default("false").Bool()
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
	config := new(auth.Config)
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

	authServer := auth.NewAuthServer(*root, config)
	address := fmt.Sprintf("%s:%s", "0.0.0.0", *port)
	listen, err := net.Listen("tcp", address)
	if err != nil {
		logrus.WithError(err).Fatal("Listen port failed")
	}
	server := grpc.NewServer()
	pa.RegisterAuthForProxyServer(server, authServer)
	logrus.WithField("address", address).Info("Server started")
	if err = server.Serve(listen); err != nil {
		logrus.WithError(err).Fatal("Server failed")
	}
}
