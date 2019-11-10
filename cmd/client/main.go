package main

import (
	"fmt"
	"io/ioutil"
	"oss/client"
	"strings"
)

func main() {
	ossClient := client.NewOSSClient()
	var cmd string
	for {
		fmt.Print("> ")
		fmt.Scanln(&cmd)
		request, hasToken, err := ossClient.Build(cmd)
		if err != nil {
			fmt.Println("build http request failed")
			continue
		}
		response, err := ossClient.Do(request)
		if err != nil {
			fmt.Println("execute http request failed")
			continue
		}
		content, err := ioutil.ReadAll(response.Body)
		if err != nil {
			response.Body.Close()
			fmt.Println("read http response body failed")
		} else if hasToken {
			ossClient.SetToken(strings.TrimSpace(string(content)))
			fmt.Println("login succeed")
		} else {
			fmt.Println(string(content))
		}
		response.Body.Close()
	}
}
