package main

import (
	"context"
	crand "crypto/rand"
	"encoding/base64"
	"fmt"
	"io/ioutil"
	"time"

	"go.etcd.io/etcd/clientv3"
)

func main() {
	cli, _ := clientv3.New(clientv3.Config{
		Endpoints:   []string{"localhost:2379"},
		DialTimeout: 5 * time.Second,
	})
	n := 1000
	keys := make([]string, n+1)
	begin := time.Now().UnixNano()
	for i := 1; i <= n; i++ {
		fmt.Println(i)
		value, _ := ioutil.ReadFile(fmt.Sprintf("%v.obj", i))
		keys[i] = randstring(5)
		cli.Put(context.Background(), keys[i], string(value))
	}
	end := time.Now().UnixNano()
	fmt.Printf("Put elapsed: %v\n", (float64)(end-begin)/1e9)
	begin = time.Now().UnixNano()
	for i := 1; i <= n; i++ {
		fmt.Println(i)
		resp, _ := cli.Get(context.Background(), keys[i])
		ioutil.WriteFile(fmt.Sprintf("%v.out", i), resp.Kvs[0].Value, 0766)
	}
	end = time.Now().UnixNano()
	fmt.Printf("Get elapsed: %v\n", (float64)(end-begin)/1e9)
	defer cli.Close()
}

func randstring(n int) string {
	b := make([]byte, 2*n)
	_, _ = crand.Read(b)
	s := base64.URLEncoding.EncodeToString(b)
	return s[0:n]
}
