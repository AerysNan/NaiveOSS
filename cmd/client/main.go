package main

import (
	"bytes"
	"crypto/sha256"
	"flag"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"

	"github.com/sirupsen/logrus"
)

var (
	endpoint = flag.String("a", "http://127.0.0.1:8082", "address of object storage service")
	method   = flag.String("m", "POST", "HTTP request method")
	bucket   = flag.String("b", "default", "bucket name")
	key      = flag.String("k", "key", "key name")
	body     = flag.String("f", "", "content file path")
)

func build() (*http.Request, error) {
	var request *http.Request
	var err error
	switch *method {
	case "POST":
		request, err = http.NewRequest(*method, *endpoint, nil)
		if err != nil {
			return nil, err
		}
		request.Header.Add("bucket", *bucket)
	case "GET":
		request, err = http.NewRequest(*method, *endpoint, nil)
		if err != nil {
			return nil, err
		}
		request.Header.Add("bucket", *bucket)
		request.Header.Add("key", *key)
	case "PUT":
		file, err := os.Open(*body)
		if err != nil {
			return nil, err
		}
		content, err := ioutil.ReadAll(file)
		if err != nil {
			return nil, err
		}
		reader := bytes.NewReader(content)
		request, err = http.NewRequest(*method, *endpoint, reader)
		if err != nil {
			return nil, err
		}
		request.Header.Add("bucket", *bucket)
		request.Header.Add("key", *key)
		request.Header.Add("tag", fmt.Sprintf("%x", sha256.Sum256(content)))
	}
	return request, nil
}

func main() {
	flag.Parse()
	client := http.Client{}
	request, err := build()
	if err != nil {
		logrus.WithError(err).Fatal("build http request failed")
	}
	response, err := client.Do(request)
	if err != nil {
		logrus.WithError(err).Fatal("execute http request failed")
	}
	defer response.Body.Close()
	content, err := ioutil.ReadAll(response.Body)
	if err != nil {
		logrus.WithError(err).Fatal("read http response body failed")
	}
	logrus.Info(string(content))
}
