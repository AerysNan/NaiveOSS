package client

import (
	"bytes"
	"crypto/sha256"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"strconv"
	"strings"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"gopkg.in/alecthomas/kingpin.v2"
)

var (
	client       = kingpin.New("client", "OSS Client")
	createBucket = client.Command("create-bucket", "Create a bucket")
	deleteBucket = client.Command("delete-bucket", "Delete a bucket")
	getObject    = client.Command("get-object", "Get object from oss")
	putObject    = client.Command("put-object", "Put object to oss")
	deleteObject = client.Command("delete-object", "Delete object in oss")
	getMetadata  = client.Command("get-metadata", "Get object meta from oss")
	grantUser    = client.Command("grant-user", "Grant user with certain privilege")
	loginUser    = client.Command("login-user", "User login")
	createUser   = client.Command("create-user", "Create new user")

	endpoint   = client.Flag("address", "address of object storage service").Short('a').Default("http://127.0.0.1:8082").String()
	bucket     = client.Flag("bucket", "bucket name").Short('b').Default("default").String()
	key        = client.Flag("key", "key name").Short('k').Default("key").String()
	body       = client.Flag("file", "content file path").Short('f').String()
	user       = client.Flag("user", "user name").Short('u').Default("admin").String()
	pass       = client.Flag("pass", "user password").Short('p').String()
	role       = client.Flag("role", "role for created user").Short('r').Int()
	permission = client.Flag("permission", "permission granted for user").Short('c').Int()
)

type OSSClient struct {
	c     *http.Client
	token string
}

func NewOSSClient() *OSSClient {
	return &OSSClient{
		c:     &http.Client{},
		token: "",
	}
}

func (c *OSSClient) Build(cmd string) (*http.Request, bool, error) {
	var request *http.Request
	var err error
	hasToken := false
	switch kingpin.MustParse(client.Parse(strings.Split(cmd, " "))) {
	case createBucket.FullCommand():
		request, err = http.NewRequest("POST", fmt.Sprintf("%s%s", *endpoint, "/api/bucket"), nil)
		if err != nil {
			return nil, false, err
		}
		request.Header.Add("bucket", *bucket)
	case deleteBucket.FullCommand():
		request, err = http.NewRequest("DELETE", fmt.Sprintf("%s%s", *endpoint, "/api/bucket"), nil)
		if err != nil {
			return nil, false, err
		}
		request.Header.Add("bucket", *bucket)
	case getObject.FullCommand():
		request, err = http.NewRequest("GET", fmt.Sprintf("%s%s", *endpoint, "/api/object"), nil)
		if err != nil {
			return nil, false, err
		}
		request.Header.Add("bucket", *bucket)
		request.Header.Add("key", *key)
	case putObject.FullCommand():
		file, err := os.Open(*body)
		if err != nil {
			return nil, false, err
		}
		content, err := ioutil.ReadAll(file)
		if err != nil {
			return nil, false, err
		}
		reader := bytes.NewReader(content)
		request, err = http.NewRequest("PUT", fmt.Sprintf("%s%s", *endpoint, "/api/object"), reader)
		if err != nil {
			return nil, false, err
		}
		request.Header.Add("bucket", *bucket)
		request.Header.Add("key", *key)
		request.Header.Add("tag", fmt.Sprintf("%x", sha256.Sum256(content)))
	case deleteObject.FullCommand():
		request, err = http.NewRequest("DELETE", fmt.Sprintf("%s%s", *endpoint, "/api/object"), nil)
		if err != nil {
			return nil, false, err
		}
		request.Header.Add("bucket", *bucket)
		request.Header.Add("key", *key)
	case getMetadata.FullCommand():
		request, err = http.NewRequest("GET", fmt.Sprintf("%s%s", *endpoint, "/api/metadata"), nil)
		if err != nil {
			return nil, false, err
		}
		request.Header.Add("bucket", *bucket)
		request.Header.Add("key", *key)
	case grantUser.FullCommand():
		request, err = http.NewRequest("POST", fmt.Sprintf("%s%s", *endpoint, "/api/auth"), nil)
		if err != nil {
			return nil, false, err
		}
		request.Header.Add("name", *user)
		request.Header.Add("bucket", *bucket)
		request.Header.Add("permission", strconv.Itoa(*permission))
	case loginUser.FullCommand():
		request, err = http.NewRequest("POST", fmt.Sprintf("%s%s", *endpoint, "/api/user"), nil)
		if err != nil {
			return nil, false, err
		}
		request.Header.Add("name", *user)
		request.Header.Add("pass", fmt.Sprintf("%x", sha256.Sum256([]byte(*pass))))
		hasToken = true
	case createUser.FullCommand():
		request, err = http.NewRequest("PUT", fmt.Sprintf("%s%s", *endpoint, "/api/auth"), nil)
		if err != nil {
			return nil, false, err
		}
		request.Header.Add("name", *user)
		request.Header.Add("pass", fmt.Sprintf("%x", sha256.Sum256([]byte(*pass))))
		request.Header.Add("role", strconv.Itoa(*role))
	default:
		return nil, false, status.Error(codes.Unimplemented, "method not implemented")
	}
	request.Header.Add("token", c.token)
	return request, hasToken, nil
}

func (c *OSSClient) Do(r *http.Request) (*http.Response, error) {
	return c.c.Do(r)
}

func (c *OSSClient) SetToken(token string) {
	c.token = token
}
