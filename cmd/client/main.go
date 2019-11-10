package main

import (
	"bytes"
	"crypto/sha256"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"oss/client"
	"strconv"
	"strings"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"gopkg.in/alecthomas/kingpin.v2"
)

var (
	app  = kingpin.New("client", "OSS Client")
	addr = app.Flag("address", "address of object storage service").Short('a').Default("http://127.0.0.1:8082").String()
	user = app.Flag("user", "user name").Short('u').Required().String()
	pass = app.Flag("pass", "user password").Short('p').Required().String()

	createBucket          = app.Command("create-bucket", "Create a bucket")
	createBucketArgBucket = createBucket.Arg("bucket", "Bucket name").Required().String()

	deleteBucket          = app.Command("delete-bucket", "Delete a bucket")
	deleteBucketArgBucket = deleteBucket.Arg("bucket", "Bucket name").Required().String()

	getObject          = app.Command("get-object", "Get object from oss")
	getObjectArgKey    = getObject.Arg("key", "Key name").Required().String()
	getObjectArgBucket = getObject.Arg("bucket", "Bucket name").Required().String()

	putObject          = app.Command("put-object", "Put object to oss")
	putObjectArgKey    = putObject.Arg("key", "Key name").Required().String()
	putObjectArgBucket = putObject.Arg("bucket", "Bucket name").Required().String()
	putObjectArgObject = putObject.Arg("object", "Object file path").Required().ExistingFile()

	deleteObject          = app.Command("delete-object", "Delete object in oss")
	deleteObjectArgKey    = deleteObject.Arg("key", "Key name").Required().String()
	deleteObjectArgBucket = deleteObject.Arg("bucket", "Bucket name").Required().String()

	getMeta          = app.Command("get-meta", "Get object meta from oss")
	getMetaArgKey    = putObject.Arg("key", "Key name").Required().String()
	getMetaArgBucket = putObject.Arg("bucket", "Bucket name").Required().String()

	grantUser              = app.Command("grant-user", "Grant user with certain privilege")
	grantUserArgUser       = grantUser.Arg("user", "User name").Required().String()
	grantUserArgBucket     = grantUser.Arg("bucket", "Bucket name").Required().String()
	grantUserArgPermission = grantUser.Arg("perm", "Permission level").Required().Int()

	createUser        = app.Command("create-user", "Create new user")
	createUserArgUser = createUser.Arg("user", "User name").Required().String()
	createUserArgPass = createUser.Arg("pass", "Password").Required().String()
	createUserArgRole = createUser.Arg("role", "Role type to determine whether superuser or not").Default("0").Int()
)

func Build(cmd string, token string) (*http.Request, error) {
	var request *http.Request
	var err error
	switch cmd {

	case createBucket.FullCommand():
		request, err = http.NewRequest("POST", fmt.Sprintf("%s%s", *addr, "/api/bucket"), nil)
		if err != nil {
			return nil, err
		}
		request.Header.Add("bucket", *createBucketArgBucket)

	case deleteBucket.FullCommand():
		request, err = http.NewRequest("DELETE", fmt.Sprintf("%s%s", *addr, "/api/bucket"), nil)
		if err != nil {
			return nil, err
		}
		request.Header.Add("bucket", *deleteBucketArgBucket)

	case getObject.FullCommand():
		request, err = http.NewRequest("GET", fmt.Sprintf("%s%s", *addr, "/api/object"), nil)
		if err != nil {
			return nil, err
		}
		request.Header.Add("bucket", *getObjectArgBucket)
		request.Header.Add("key", *getObjectArgKey)

	case putObject.FullCommand():
		file, err := os.Open(*putObjectArgObject)
		if err != nil {
			return nil, err
		}
		content, err := ioutil.ReadAll(file)
		if err != nil {
			return nil, err
		}
		reader := bytes.NewReader(content)
		request, err = http.NewRequest("PUT", fmt.Sprintf("%s%s", *addr, "/api/object"), reader)
		if err != nil {
			return nil, err
		}
		request.Header.Add("bucket", *putObjectArgBucket)
		request.Header.Add("key", *putObjectArgKey)
		request.Header.Add("tag", fmt.Sprintf("%x", sha256.Sum256(content)))

	case deleteObject.FullCommand():
		request, err = http.NewRequest("DELETE", fmt.Sprintf("%s%s", *addr, "/api/object"), nil)
		if err != nil {
			return nil, err
		}
		request.Header.Add("bucket", *deleteObjectArgBucket)
		request.Header.Add("key", *deleteObjectArgKey)

	case getMeta.FullCommand():
		request, err = http.NewRequest("GET", fmt.Sprintf("%s%s", *addr, "/api/metadata"), nil)
		if err != nil {
			return nil, err
		}
		request.Header.Add("bucket", *getMetaArgBucket)
		request.Header.Add("key", *getMetaArgKey)

	case grantUser.FullCommand():
		request, err = http.NewRequest("POST", fmt.Sprintf("%s%s", *addr, "/api/auth"), nil)
		if err != nil {
			return nil, err
		}
		request.Header.Add("name", *grantUserArgUser)
		request.Header.Add("bucket", *grantUserArgBucket)
		request.Header.Add("permission", strconv.Itoa(*grantUserArgPermission))

	case createUser.FullCommand():
		request, err = http.NewRequest("PUT", fmt.Sprintf("%s%s", *addr, "/api/auth"), nil)
		if err != nil {
			return nil, err
		}
		request.Header.Add("name", *createUserArgUser)
		request.Header.Add("pass", fmt.Sprintf("%x", sha256.Sum256([]byte(*createUserArgPass))))
		request.Header.Add("role", strconv.Itoa(*createUserArgRole))

	default:
		return nil, status.Error(codes.Unimplemented, "method not implemented")
	}

	request.Header.Add("token", token)
	return request, nil
}

func main() {
	ossClient := client.NewOSSClient()
	request, err := http.NewRequest("POST", fmt.Sprintf("%s%s", *addr, "/api/user"), nil)
	if err != nil {
		fmt.Println("Error: build login request failed, quit")
		return
	}
	request.Header.Add("name", *user)
	request.Header.Add("pass", fmt.Sprintf("%x", sha256.Sum256([]byte(*pass))))
	response, err := ossClient.Do(request)
	if err != nil {
		fmt.Println("Error: send login request failed, quit")
		return
	}
	content, err := ioutil.ReadAll(response.Body)
	if err != nil {
		fmt.Println("Error: read login response failed, quit")
		return
	}
	ossClient.Token = string(content)
	fmt.Println("=====Login succeeded!=====\n==========================")

	var cmd string
	for {
		fmt.Print("> ")
		fmt.Scanln(&cmd)
		command, err := app.Parse(strings.Split(cmd, " "))
		if err != nil {
			fmt.Println("Error: invalid oss command")
		}
		request, err := Build(command, ossClient.Token)
		if err != nil {
			fmt.Println("Error: build oss request failed")
			continue
		}
		response, err := ossClient.Do(request)
		if err != nil {
			fmt.Println("Error: execute oss request failed")
			continue
		}
		content, err := ioutil.ReadAll(response.Body)
		if err != nil {
			fmt.Println("Error: read oss response body failed")
		} else if response.StatusCode != http.StatusOK {
			fmt.Printf("Error: %v\n", string(content))
		} else {
			fmt.Println(string(content))
		}
		response.Body.Close()
	}
}
