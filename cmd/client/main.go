package main

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"

	"github.com/sirupsen/logrus"
	"gopkg.in/alecthomas/kingpin.v2"
)

var (
	client       = kingpin.New("oss", "OSS Client")
	createBucket = client.Command("create-bucket", "Create a bucket")
	deleteBucket = client.Command("delete-bucket", "Delete a bucket")
	getObject    = client.Command("get-object", "Get object from oss")
	putObject    = client.Command("put-object", "Put object to oss")
	deleteObject = client.Command("delete-object", "Delete object in oss")
	getMetadata  = client.Command("get-metadata", "Get object meta from oss")
	endpoint     = client.Flag("a", "address of object storage service").Default("http://127.0.0.1:8082").String()
	bucket       = client.Flag("b", "bucket name").Default("default").String()
	key          = client.Flag("k", "key name").Default("key").String()
	body         = client.Flag("f", "content file path").Default("").String()
)

func build() (*http.Request, error) {
	var request *http.Request
	var err error
	switch kingpin.MustParse(client.Parse(os.Args[1:])) {
	case createBucket.FullCommand():
		request, err = http.NewRequest("POST", fmt.Sprintf("%s%s", *endpoint, "/api/bucket"), nil)
		if err != nil {
			return nil, err
		}
		request.Header.Add("bucket", *bucket)
	case deleteBucket.FullCommand():
		request, err = http.NewRequest("DELETE", fmt.Sprintf("%s%s", *endpoint, "/api/bucket"), nil)
		if err != nil {
			return nil, err
		}
		request.Header.Add("bucket", *bucket)
	case getObject.FullCommand():
		request, err = http.NewRequest("GET", fmt.Sprintf("%s%s", *endpoint, "/api/object"), nil)
		if err != nil {
			return nil, err
		}
		request.Header.Add("bucket", *bucket)
		request.Header.Add("key", *key)
	case putObject.FullCommand():
		file, err := os.Open(*body)
		if err != nil {
			return nil, err
		}
		content, err := ioutil.ReadAll(file)
		if err != nil {
			return nil, err
		}
		reader := bytes.NewReader(content)
		request, err = http.NewRequest("PUT", fmt.Sprintf("%s%s", *endpoint, "/api/object"), reader)
		if err != nil {
			return nil, err
		}
		request.Header.Add("bucket", *bucket)
		request.Header.Add("key", *key)
		request.Header.Add("tag", fmt.Sprintf("%x", sha256.Sum256(content)))
	case deleteObject.FullCommand():
		request, err = http.NewRequest("DELETE", fmt.Sprintf("%s%s", *endpoint, "/api/object"), nil)
		if err != nil {
			return nil, err
		}
		request.Header.Add("bucket", *bucket)
		request.Header.Add("key", *key)
	case getMetadata.FullCommand():
		request, err = http.NewRequest("GET", fmt.Sprintf("%s%s", *endpoint, "/api/metadata"), nil)
		if err != nil {
			return nil, err
		}
		request.Header.Add("bucket", *bucket)
		request.Header.Add("key", *key)
	default:
		return nil, errors.New("method not implemented")
	}
	return request, nil
}

func main() {
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
