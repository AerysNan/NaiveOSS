package main

import (
	"bytes"
	"crypto/sha256"
	"fmt"
	"io/ioutil"
	"math/rand"
	"net/http"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
	"gopkg.in/alecthomas/kingpin.v2"
)

var (
	quantity = kingpin.Flag("q", "Plz input client quantity").Default("5").Int()
	times    = kingpin.Flag("t", "Plz input times quantity").Default("5").Int()
	endpoint = kingpin.Flag("a", "address of object storage service").Default("http://127.0.0.1:8082").String()
)

var (
	defaultBucket = "default"
	total         = 0.0
	about         = 0.0
	success       = 0.0
	failure       = 0.0
	use_time      = 0.0
)

func serverInit() error {
	client := http.Client{}
	request, err := http.NewRequest("POST", fmt.Sprintf("%s%s", *endpoint, "/api/bucket"), nil)
	if err != nil {
		logrus.WithError(err).Fatal("build http request failed")
		return err
	}
	request.Header.Add("bucket", defaultBucket)
	response, err := client.Do(request)
	if err != nil {
		logrus.WithError(err).Fatal("execute http request failed")
	}
	defer response.Body.Close()
	_, err = ioutil.ReadAll(response.Body)
	if err != nil {
		logrus.WithError(err).Fatal("read http response body failed")
	}
	for i := 1; i <= 10; i++ {
		w.Add(1)
		go func(key int) {
			defer w.Done()
			path := fmt.Sprintf("./%d.obj", key)
			file, err := os.Open(path)
			if err != nil {
				logrus.WithError(err).Fatal("open file failed")
			}
			content, err := ioutil.ReadAll(file)
			if err != nil {
				logrus.WithError(err).Fatal("read file failed")
			}
			c := http.Client{}
			reader := bytes.NewReader(content)
			request, err := http.NewRequest("PUT", fmt.Sprintf("%s%s", *endpoint, "/api/object"), reader)
			if err != nil {
				logrus.WithError(err).Fatal("execute http request failed")
			}
			request.Header.Add("bucket", defaultBucket)
			request.Header.Add("key", strconv.Itoa(key))
			request.Header.Add("tag", fmt.Sprintf("%x", sha256.Sum256(content)))
			response, err := c.Do(request)
			if err != nil {
				logrus.WithError(err).Fatal("execute http request failed")
			}
			defer response.Body.Close()
			_, err = ioutil.ReadAll(response.Body)
			if err != nil {
				logrus.WithError(err).Fatal("read http response body failed")
			}
		}(i)
	}
	w.Wait()
	return nil
}

var w sync.WaitGroup
var wg sync.WaitGroup

func run(num int) {

	defer wg.Done()

	no := 0.0
	ok := 0.0
	client := http.Client{}
	for i := 0; i < num; i++ {
		request, err := http.NewRequest("GET", fmt.Sprintf("%s%s", *endpoint, "/api/object"), nil)
		if err != nil {
			logrus.WithError(err).Error("build http request failed")
			no++
			continue
		}
		request.Header.Add("bucket", defaultBucket)
		index := rand.Intn(10) + 1
		request.Header.Add("key", strconv.Itoa(index))
		response, err := client.Do(request)
		if err != nil {
			logrus.WithError(err).Error("execute http request failed")
			no++
			continue
		}
		defer response.Body.Close()
		if response.StatusCode != 200 {
			logrus.WithError(err).Error("http response status invalid")
			no++
			continue
		}
		content, err := ioutil.ReadAll(response.Body)
		if err != nil {
			logrus.WithError(err).Error("read http response body failed")
			no++
			continue
		}
		path := fmt.Sprintf("./%d.obj", index)
		file, err := os.Open(path)
		if err != nil {
			logrus.WithError(err).Fatal("open file failed")
		}
		localContent, err := ioutil.ReadAll(file)
		if err != nil {
			logrus.WithError(err).Fatal("read file failed")
		}
		if string(localContent) != string(content) {
			logrus.WithError(err).Fatal("wrong content")
			no++
			continue
		}
		ok++
	}

	success += ok
	failure += no
	total += float64(num)

}

func main() {

	start_time := time.Now().UnixNano()

	kingpin.Parse()

	serverInit()
	for i := 0; i < *quantity; i++ {
		wg.Add(1)
		go run(*times)
	}

	wg.Wait()
	end_time := time.Now().UnixNano()

	fmt.Println("PreTotal:", (*quantity)*(*times))
	fmt.Println("Total:", total)
	fmt.Println("Success:", success)
	fmt.Println("Failure:", failure)
	fmt.Println("SuccessRate:", fmt.Sprintf("%.2f", ((success/total)*100.0)), "%")
	fmt.Println("UseTime:", fmt.Sprintf("%.4f", float64(end_time-start_time)/1e9), "s")
}
