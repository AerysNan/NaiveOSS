package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"oss/global"
	"path"
	"strconv"

	"github.com/natefinch/atomic"
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

	listBucket = app.Command("list-bucket", "List all buckets")

	deleteBucket           = app.Command("delete-bucket", "Delete a bucket")
	deleteBucketFlagBucket = deleteBucket.Flag("bucket", "Bucket name").Short('b').Required().String()

	listObject           = app.Command("ls", "list all keys")
	listObjectFlagBucket = listObject.Flag("bucket", "Bucket name").Short('b').Required().String()

	getObject           = app.Command("get", "Get object from oss")
	getObjectFlagBucket = getObject.Flag("bucket", "Bucket name").Short('b').Required().String()
	getObjectFlagKey    = getObject.Flag("key", "Key name").Short('k').Required().String()

	rangeObject            = app.Command("range", "List a range of keys from oss")
	rangeObjectFlagBucket  = rangeObject.Flag("bucket", "Bucket name").Short('b').Required().String()
	rangeObjectFlagFromKey = rangeObject.Flag("from", "Key range lower bound").Short('l').Required().String()
	rangeObjectFlagToKey   = rangeObject.Flag("to", "Key range higher bound").Short('r').Required().String()

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

type Task struct {
	Id    string
	Index int64
}

var (
	errorSaveToken      = errors.New("save token file failed")
	errorSaveObject     = errors.New("save object file failed")
	errorReadTaskFile   = errors.New("read task file failed")
	errorParseTaskFile  = errors.New("parse task file failed")
	errorReadResponse   = errors.New("read response body failed")
	errorBuildRequest   = errors.New("build http request failed")
	errorReadObjectFile = errors.New("read object file failed")
	errorPutObjectFile  = errors.New("put object file failed")
	errorExecuteRequest = errors.New("execute http request failed")
	errorNotImplemented = errors.New("method not implemented")
)

func handle(client *http.Client, cmd string, token string) (*Response, error) {
	var request *http.Request
	var err error
	hasToken := false
	getFile := false

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

	case listBucket.FullCommand():
		request, err = http.NewRequest("GET", fmt.Sprintf("%s%s", *addr, "/api/bucket"), nil)
		if err != nil {
			return nil, errorBuildRequest
		}

	case listObject.FullCommand():
		request, err = http.NewRequest("POST", fmt.Sprintf("%s%s", *addr, "/api/object"), nil)
		if err != nil {
			return nil, errorBuildRequest
		}
		request.Header.Add("bucket", *listObjectFlagBucket)

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
		file, err := os.Stat(*getObjectFlagKey)
		var start int64
		if err != nil {
			start = 0
		} else {
			start = file.Size()
			fmt.Printf("start downloading object from offset %d\n", start)
		}
		getFile = true
		request, err = http.NewRequest("GET", fmt.Sprintf("%s%s", *addr, "/api/object"), nil)
		if err != nil {
			return nil, errorBuildRequest
		}
		request.Header.Add("bucket", *getObjectFlagBucket)
		request.Header.Add("key", *getObjectFlagKey)
		request.Header.Add("start", strconv.FormatInt(start, 10))

	case putObject.FullCommand():
		file, err := os.Open(*putObjectFlagObject)
		_, fileName := path.Split(*putObjectFlagObject)
		if err != nil {
			return nil, errorReadObjectFile
		}
		content, err := ioutil.ReadAll(file)
		if err != nil {
			return nil, errorReadObjectFile
		}
		tag := fmt.Sprintf("%x", sha256.Sum256(content))
		r := &Response{}
		t := &Task{Id: "0"}
		name := fmt.Sprintf("tmp_%s", *putObjectFlagKey)
		file, err = os.Open(name)
		if err == nil {
			bytes, err := ioutil.ReadAll(file)
			if err != nil {
				return nil, errorReadTaskFile
			}
			file.Close()
			err = json.Unmarshal(bytes, &t)
			if err != nil {
				return nil, errorParseTaskFile
			}
		}
		request, err = http.NewRequest("POST", fmt.Sprintf("%s%s", *addr, "/api/task"), nil)
		if err != nil {
			return nil, errorBuildRequest
		}
		request.Header.Add("bucket", *putObjectFlagBucket)
		request.Header.Add("name", fileName)
		request.Header.Add("key", *putObjectFlagKey)
		request.Header.Add("tag", tag)
		request.Header.Add("token", token)
		request.Header.Add("id", t.Id)
		response, err := client.Do(request)
		if err != nil {
			return nil, errorPutObjectFile
		}
		id, err := ioutil.ReadAll(response.Body)
		response.Body.Close()
		if err != nil {
			return nil, errorReadResponse
		}
		if string(id) == "0" {
			_ = os.Remove(name)
			r.code = http.StatusOK
			r.body = "OK"
			return r, nil
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
			request, err = http.NewRequest("PUT", fmt.Sprintf("%s%s", *addr, "/api/object"), reader)
			if err != nil {
				return nil, err
			}
			request.Header.Add("id", t.Id)
			request.Header.Add("bucket", *putObjectFlagBucket)
			request.Header.Add("offset", strconv.FormatInt(offset, 10))
			request.Header.Add("token", token)
			response, err := client.Do(request)
			if err != nil {
				return nil, err
			}
			if response.StatusCode != http.StatusOK {
				_ = os.Remove(name)
				content, err := ioutil.ReadAll(response.Body)
				if err != nil {
					return nil, err
				}
				return nil, errors.New(string(content))
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
		request, err = http.NewRequest("DELETE", fmt.Sprintf("%s%s", *addr, "/api/task"), nil)
		if err != nil {
			return nil, errorBuildRequest
		}
		_ = os.Remove(name)
		request.Header.Add("id", t.Id)
		request.Header.Add("name", fileName)
		request.Header.Add("bucket", *putObjectFlagBucket)
		request.Header.Add("key", *putObjectFlagKey)
		request.Header.Add("tag", tag)

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

	case rangeObject.FullCommand():
		request, err = http.NewRequest("POST", fmt.Sprintf("%s%s", *addr, "/api/metadata"), nil)
		if err != nil {
			return nil, errorBuildRequest
		}
		request.Header.Add("bucket", *rangeObjectFlagBucket)
		request.Header.Add("from", *rangeObjectFlagFromKey)
		request.Header.Add("to", *rangeObjectFlagToKey)

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
		return nil, errorNotImplemented
	}

	request.Header.Add("token", token)
	response, err := client.Do(request)
	if err != nil {
		return nil, errorExecuteRequest
	}
	defer response.Body.Close()
	r := &Response{code: response.StatusCode}
	if getFile && response.StatusCode == http.StatusOK {
		path := response.Header.Get("name")
		for {
			bytes := make([]byte, global.MaxChunkSize)
			count, err := response.Body.Read(bytes)
			if err == io.EOF {
				if count != 0 {
					err = saveFile(bytes[:count], path)
					if err != nil {
						return nil, err
					}
				}
				break
			}
			err = saveFile(bytes[:count], path)
			if err != nil {
				return nil, err
			}
		}
		r.body = fmt.Sprintf("The file has been saved to file %s.", path)
		return r, nil
	}
	bytes, err := ioutil.ReadAll(response.Body)
	if err != nil {
		return nil, errorReadResponse
	}
	if hasToken && response.StatusCode == http.StatusOK {
		err = saveToken(string(bytes))
		if err != nil {
			return nil, err
		}
		r.body = "OK"
		return r, nil
	}
	r.body = string(bytes)
	return r, nil
}

func getToken() string {
	file, err := os.Open("token")
	if err != nil {
		return "undefined"
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		return "undefined"
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

func saveFile(content []byte, filename string) error {
	file, err := os.OpenFile(filename, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0766)
	if err != nil {
		return errorSaveObject
	}
	defer file.Close()
	_, err = file.Write(content)
	if err != nil {
		return errorSaveObject
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
	} else if len(response.body) != 0 {
		fmt.Println(string(response.body))
	}
}
