package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"oss/global"
	"strconv"
	"sync"
	"time"

	"github.com/natefinch/atomic"
	"gopkg.in/alecthomas/kingpin.v2"
)

var (
	endpoint = kingpin.Flag("a", "address of object storage service").Default("http://127.0.0.1:8082").String()
)

type Test struct {
	token         string
	defaultBucket string
	success       float32
	failure       float32
	w             sync.WaitGroup
}

type Response struct {
	code int
	body string
}

type Task struct {
	Id    string
	Index int64
}

func newTest() *Test {
	return &Test{
		token:         "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJuYW1lIjoiYWRtaW4iLCJyb2xlIjoxfQ.qpYMI3BCWVYRl2UGa98Z4vzcZUhx-LxeZ5hrDCXiJ_s",
		defaultBucket: "default",
		success:       0.0,
		failure:       0.0,
	}
}

func (test *Test) Put(key, filepath string) error {
	client := &http.Client{}
	putObjectFlagKey := &key
	putObjectFlagObject := &filepath
	file, err := os.Open(*putObjectFlagObject)
	if err != nil {
		return err
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		return err
	}
	tag := fmt.Sprintf("%x", sha256.Sum256(content))
	r := &Response{}
	t := &Task{Id: "0"}
	name := fmt.Sprintf("tmp_%s", *putObjectFlagKey)
	file, err = os.Open(name)
	if err == nil {
		bytes, err := ioutil.ReadAll(file)
		if err != nil {
			return err
		}
		file.Close()
		err = json.Unmarshal(bytes, &t)
		if err != nil {
			return err
		}
	}
	request, err := http.NewRequest("POST", fmt.Sprintf("%s%s", *endpoint, "/api/task"), nil)
	if err != nil {
		return err
	}
	request.Header.Add("bucket", test.defaultBucket)
	request.Header.Add("key", *putObjectFlagKey)
	request.Header.Add("tag", tag)
	request.Header.Add("token", test.token)
	request.Header.Add("id", t.Id)
	response, err := client.Do(request)
	if err != nil {
		return err
	}
	id, err := ioutil.ReadAll(response.Body)
	response.Body.Close()
	if err != nil {
		return err
	}
	if string(id) == "0" {
		_ = os.Remove(name)
		r.code = http.StatusOK
		r.body = "OK"
		return nil
	}
	if t.Id != string(id) {
		t.Id = string(id)
		t.Index = 0
	}
	offset := t.Index * global.MaxChunkSize
	for offset < int64(len(content)) {
		end := offset + global.MaxChunkSize
		if end > int64(len(content)) {
			end = int64(len(content))
		}
		reader := bytes.NewReader(content[offset:end])
		request, err = http.NewRequest("PUT", fmt.Sprintf("%s%s", *endpoint, "/api/object"), reader)
		if err != nil {
			return err
		}
		request.Header.Add("id", t.Id)
		request.Header.Add("bucket", test.defaultBucket)
		request.Header.Add("tag", tag)
		request.Header.Add("offset", strconv.FormatInt(offset, 10))
		request.Header.Add("token", test.token)
		response, err := client.Do(request)
		if err != nil {
			return err
		}
		if response.StatusCode != http.StatusOK {
			_ = os.Remove(name)
			return os.ErrInvalid
		}
		t.Index++
		offset += global.MaxChunkSize
		data, err := json.Marshal(t)
		if err != nil {
			continue
		}
		err = atomic.WriteFile(name, bytes.NewReader(data))
		if err != nil {
			continue
		}
	}
	request, err = http.NewRequest("DELETE", fmt.Sprintf("%s%s", *endpoint, "/api/task"), nil)
	if err != nil {
		return err
	}
	_ = os.Remove(name)
	request.Header.Add("id", t.Id)
	request.Header.Add("bucket", test.defaultBucket)
	request.Header.Add("key", *putObjectFlagKey)
	request.Header.Add("tag", tag)
	request.Header.Add("token", test.token)
	response, err = client.Do(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	_, err = ioutil.ReadAll(response.Body)
	if err != nil {
		return err
	}
	return nil
}

func (test *Test) Get(key string) error {
	client := &http.Client{}
	getObjectFlagKey := &key
	file, err := os.Stat(*getObjectFlagKey)
	var start int64
	if err != nil {
		start = 0
	} else {
		start = file.Size()
		fmt.Printf("start downloading object from offset %d\n", start)
	}
	request, err := http.NewRequest("GET", fmt.Sprintf("%s%s", *endpoint, "/api/object"), nil)
	if err != nil {
		return err
	}
	request.Header.Add("bucket", test.defaultBucket)
	request.Header.Add("key", *getObjectFlagKey)
	request.Header.Add("start", strconv.FormatInt(start, 10))
	request.Header.Add("token", test.token)
	response, err := client.Do(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	bytes, err := ioutil.ReadAll(response.Body)
	if err != nil {
		return err
	}
	r := &Response{
		code: response.StatusCode,
		body: string(bytes),
	}
	path := fmt.Sprintf("%s.oss", *getObjectFlagKey)
	if response.StatusCode == http.StatusOK {
		err = saveFile(bytes, path)
		if err != nil {
			return err
		}
		r.body = fmt.Sprintf("The file has been saved to file %s", path)
	} else if response.StatusCode == http.StatusForbidden {
		r.code = http.StatusOK
		r.body = fmt.Sprintf("The file %s already exists.", path)
	}

	return nil
}

func saveFile(content []byte, filename string) error {
	file, err := os.OpenFile(filename, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0766)
	if err != nil {
		return err
	}
	defer file.Close()
	_, err = file.Write(content)
	if err != nil {
		return err
	}
	return nil
}

func (t *Test) parallelTest() error {
	client := &http.Client{}
	request, err := http.NewRequest("POST", fmt.Sprintf("%s%s", *endpoint, "/api/bucket"), nil)
	if err != nil {
		return err
	}
	request.Header.Add("bucket", t.defaultBucket)
	request.Header.Add("token", t.token)
	_, err = client.Do(request)
	if err != nil {
		return err
	}
	for i := 1; i <= 10; i++ {
		t.w.Add(1)
		go func(key int) {
			defer t.w.Done()
			err := t.Put(fmt.Sprintf("%d", key), fmt.Sprintf("%d.obj", key))
			if err != nil {
				fmt.Println(err)
			}
		}(i)
	}
	t.w.Wait()
	for i := 1; i <= 10; i++ {
		t.w.Add(1)
		go func(key int) {
			defer t.w.Done()
			err := t.Get(fmt.Sprintf("%d", key))
			if err != nil {
				fmt.Println(err)
			}
		}(i)
	}
	t.w.Wait()
	for i := 1; i <= 10; i++ {
		file1, err := os.Open(fmt.Sprintf("%d.oss", i))
		if err != nil {
			t.failure++
			continue
		}
		content1, err := ioutil.ReadAll(file1)
		if err != nil {
			t.failure++
			_ = os.Remove(fmt.Sprintf("%d.oss", i))
			continue
		}
		_ = os.Remove(fmt.Sprintf("%d.oss", i))
		file2, err := os.Open(fmt.Sprintf("%d.obj", i))
		if err != nil {
			t.failure++
			continue
		}
		content2, err := ioutil.ReadAll(file2)
		if err != nil {
			t.failure++
			continue
		}
		if string(content1) != string(content2) {
			t.failure++
		} else {
			t.success++
		}
	}
	return nil
}

func main() {
	kingpin.Parse()
	test := newTest()
	startTime := time.Now().UnixNano()
	err := test.parallelTest()
	if err != nil {
		fmt.Println("Test failed")
		return
	}
	endTime := time.Now().UnixNano()
	fmt.Println("Success:", test.success)
	fmt.Println("Failure:", test.failure)
	fmt.Println("SuccessRate:", fmt.Sprintf("%.2f", ((test.success/(test.success+test.failure))*100.0)), "%")
	fmt.Println("UseTime:", fmt.Sprintf("%.4f", float64(endTime-startTime)/1e9), "s")
}
