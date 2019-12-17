package main

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"strconv"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"gopkg.in/alecthomas/kingpin.v2"
)

var (
	app  = kingpin.New("client", "OSS Client")
	addr = app.Flag("address", "address of object storage service").Short('a').Default("http://127.0.0.1:8082").String()

	loginUser         = app.Command("login", "User login")
	loginUserFlagUser = loginUser.Flag("user", "User name").Short('u').Required().String()
	loginUserFlagPass = loginUser.Flag("pass", "Password").Short('p').Required().String()

	logoutUser = app.Command("logout", "User logout")

	createBucket           = app.Command("create-bucket", "Create a bucket")
	createBucketFlagBucket = createBucket.Flag("bucket", "Bucket name").Short('b').Required().String()

	deleteBucket           = app.Command("delete-bucket", "Delete a bucket")
	deleteBucketFlagBucket = deleteBucket.Flag("bucket", "Bucket name").Short('b').Required().String()

	getObject           = app.Command("get", "Get object from oss")
	getObjectFlagBucket = getObject.Flag("bucket", "Bucket name").Short('b').Required().String()
	getObjectFlagKey    = getObject.Flag("key", "Key name").Short('k').Required().String()

	putObject           = app.Command("put", "Put object to oss")
	putObjectFlagBucket = putObject.Flag("bucket", "Bucket name").Short('b').Required().String()
	putObjectFlagKey    = putObject.Flag("key", "Key name").Short('k').Required().String()
	putObjectFlagObject = putObject.Flag("file", "Object file path").Short('f').Required().String()

	deleteObject           = app.Command("delete", "Delete object in oss")
	deleteObjectFlagBucket = deleteObject.Flag("bucket", "Bucket name").Short('b').Required().String()
	deleteObjectFlagKey    = deleteObject.Flag("key", "Key name").Short('k').Required().String()

	getMeta           = app.Command("head", "Get object meta from oss")
	getMetaFlagBucket = getMeta.Flag("bucket", "Bucket name").Short('b').Required().String()
	getMetaFlagKey    = getMeta.Flag("key", "Key name").Short('k').Required().String()

	grantUser               = app.Command("grant", "Grant user with certain privilege")
	grantUserFlagUser       = grantUser.Flag("user", "User name").Short('u').Required().String()
	grantUserFlagBucket     = grantUser.Flag("bucket", "Bucket name").Short('b').Required().String()
	grantUserFlagPermission = grantUser.Flag("level", "Permission level").Short('l').Required().Int()

	createUser         = app.Command("register", "Create new user")
	createUserFlagUser = createUser.Flag("user", "User name").Short('u').Required().String()
	createUserFlagPass = createUser.Flag("pass", "Password").Short('p').Required().String()
	createUserFlagRole = createUser.Flag("role", "Role type to determine whether superuser or not").Short('r').Default("0").Int()
)

// Response represents an OSS response
type Response struct {
	code int
	body string
}

var (
	errorSaveToken      = errors.New("save token file failed")
	errorReadResponse   = errors.New("read response body failed")
	errorBuildRequest   = errors.New("build http request failed")
	errorReadObjectFile = errors.New("read object file failed")
	errorExecuteRequest = errors.New("execute http request failed")
)

func handle(client *http.Client, cmd string, token string) (*Response, error) {
	var request *http.Request
	var err error
	hasToken := false

	switch cmd {
	case loginUser.FullCommand():
		request, err = http.NewRequest("POST", fmt.Sprintf("%s%s", *addr, "/api/user"), nil)
		if err != nil {
			return nil, errorBuildRequest
		}
		hasToken = true
		request.Header.Add("name", *loginUserFlagUser)
		request.Header.Add("pass", fmt.Sprintf("%x", sha256.Sum256([]byte(*loginUserFlagPass))))

	case logoutUser.FullCommand():
		os.Remove("token")
		return &Response{
			code: http.StatusOK,
			body: "OK",
		}, nil

	case createBucket.FullCommand():
		request, err = http.NewRequest("POST", fmt.Sprintf("%s%s", *addr, "/api/bucket"), nil)
		if err != nil {
			return nil, errorBuildRequest
		}
		request.Header.Add("bucket", *createBucketFlagBucket)

	case deleteBucket.FullCommand():
		request, err = http.NewRequest("DELETE", fmt.Sprintf("%s%s", *addr, "/api/bucket"), nil)
		if err != nil {
			return nil, errorBuildRequest
		}
		request.Header.Add("bucket", *deleteBucketFlagBucket)

	case getObject.FullCommand():
		request, err = http.NewRequest("GET", fmt.Sprintf("%s%s", *addr, "/api/object"), nil)
		if err != nil {
			return nil, errorBuildRequest
		}
		request.Header.Add("bucket", *getObjectFlagBucket)
		request.Header.Add("key", *getObjectFlagKey)

	case putObject.FullCommand():
		file, err := os.Open(*putObjectFlagObject)
		if err != nil {
			return nil, errorReadObjectFile
		}
		content, err := ioutil.ReadAll(file)
		if err != nil {
			return nil, errorReadObjectFile
		}
		reader := bytes.NewReader(content)
		request, err = http.NewRequest("PUT", fmt.Sprintf("%s%s", *addr, "/api/object"), reader)
		if err != nil {
			return nil, errorBuildRequest
		}
		request.Header.Add("bucket", *putObjectFlagBucket)
		request.Header.Add("key", *putObjectFlagKey)
		request.Header.Add("tag", fmt.Sprintf("%x", sha256.Sum256(content)))

	case deleteObject.FullCommand():
		request, err = http.NewRequest("DELETE", fmt.Sprintf("%s%s", *addr, "/api/object"), nil)
		if err != nil {
			return nil, errorBuildRequest
		}
		request.Header.Add("bucket", *deleteObjectFlagBucket)
		request.Header.Add("key", *deleteObjectFlagKey)

	case getMeta.FullCommand():
		request, err = http.NewRequest("GET", fmt.Sprintf("%s%s", *addr, "/api/metadata"), nil)
		if err != nil {
			return nil, errorBuildRequest
		}
		request.Header.Add("bucket", *getMetaFlagBucket)
		request.Header.Add("key", *getMetaFlagKey)

	case grantUser.FullCommand():
		request, err = http.NewRequest("POST", fmt.Sprintf("%s%s", *addr, "/api/auth"), nil)
		if err != nil {
			return nil, errorBuildRequest
		}
		request.Header.Add("name", *grantUserFlagUser)
		request.Header.Add("bucket", *grantUserFlagBucket)
		request.Header.Add("permission", strconv.Itoa(*grantUserFlagPermission))

	case createUser.FullCommand():
		request, err = http.NewRequest("PUT", fmt.Sprintf("%s%s", *addr, "/api/user"), nil)
		if err != nil {
			return nil, errorBuildRequest
		}
		request.Header.Add("name", *createUserFlagUser)
		request.Header.Add("pass", fmt.Sprintf("%x", sha256.Sum256([]byte(*createUserFlagPass))))
		request.Header.Add("role", strconv.Itoa(*createUserFlagRole))

	default:
		return nil, status.Error(codes.Unimplemented, "method not implemented")
	}

	request.Header.Add("token", token)
	response, err := client.Do(request)
	if err != nil {
		return nil, errorExecuteRequest
	}
	defer response.Body.Close()
	bytes, err := ioutil.ReadAll(response.Body)
	if err != nil {
		return nil, errorReadResponse
	}
	r := &Response{
		code: response.StatusCode,
		body: string(bytes),
	}
	if hasToken && response.StatusCode == http.StatusOK {
		err = saveToken(r.body)
		if err != nil {
			return nil, err
		}
		r.body = "OK"
	}
	return r, nil
}

func getToken() string {
	file, err := os.Open("token")
	if err != nil {
		return ""
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		return ""
	}
	return string(content)
}

func saveToken(token string) error {
	file, err := os.OpenFile("token", os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0766)
	if err != nil {
		return errorSaveToken
	}
	defer file.Close()
	_, err = file.Write([]byte(token))
	if err != nil {
		return errorSaveToken
	}
	return nil
}

func main() {
	client := &http.Client{}
	token := getToken()
	command, err := app.Parse(os.Args[1:])
	if err != nil {
		fmt.Printf("Error: %v, invalid oss command\n", err)
		return
	}
	response, err := handle(client, command, token)
	if err != nil {
		fmt.Printf("Error: %v\n", err)
	} else if response.code != http.StatusOK {
		fmt.Printf("Error: %v\n", string(response.body))
	} else {
		fmt.Println(string(response.body))
	}
}
