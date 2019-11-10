package proxy

import (
	"context"
	"crypto/sha256"
	"fmt"
	"io/ioutil"
	"net/http"
	"strconv"
	"sync"

	"oss/global"
	pa "oss/proto/auth"
	pm "oss/proto/metadata"
	ps "oss/proto/storage"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var maxStorageConnection = 10

type ProxyServer struct {
	http.Handler
	authClient  pa.AuthForProxyClient
	metaClient  pm.MetadataForProxyClient
	dataClients map[string]*grpc.ClientConn
	m           *sync.RWMutex
	address     string
}

func NewProxyServer(address string, authClient pa.AuthForProxyClient, metadataClient pm.MetadataForProxyClient) *ProxyServer {
	return &ProxyServer{
		address:     address,
		m:           new(sync.RWMutex),
		authClient:  authClient,
		metaClient:  metadataClient,
		dataClients: make(map[string]*grpc.ClientConn),
	}
}

func (s *ProxyServer) createBucket(w http.ResponseWriter, r *http.Request) {
	p, err := checkParameter(r, []string{"bucket", "token"})
	if err != nil {
		writeError(w, err)
		return
	}
	bucket, token := p["bucket"], p["token"]
	_, err = s.authClient.Check(context.Background(), &pa.CheckRequest{
		Token:      token,
		Bucket:     bucket,
		Permission: global.PermissionNone,
	})
	if err != nil {
		writeError(w, err)
		return
	}
	_, err = s.metaClient.CreateBucket(context.Background(), &pm.CreateBucketRequest{
		Bucket: bucket,
	})
	if err != nil {
		writeError(w, err)
		return
	}
	_, err = s.authClient.Confirm(context.Background(), &pa.ConfirmRequest{
		Token:  token,
		Bucket: bucket,
	})
	if err != nil {
		writeError(w, err)
		return
	}
	writeResponse(w, nil)
}

func (s *ProxyServer) deleteBucket(w http.ResponseWriter, r *http.Request) {
	p, err := checkParameter(r, []string{"bucket", "token"})
	if err != nil {
		writeError(w, err)
	}
	bucket, token := p["bucket"], p["token"]
	_, err = s.authClient.Check(context.Background(), &pa.CheckRequest{
		Token:      token,
		Bucket:     bucket,
		Permission: global.PermissionOwner,
	})
	if err != nil {
		writeError(w, err)
		return
	}
	_, err = s.metaClient.DeleteBucket(context.Background(), &pm.DeleteBucketRequest{
		Bucket: bucket,
	})
	if err != nil {
		writeError(w, err)
		return
	}
	_, err = s.authClient.Clear(context.Background(), &pa.ClearRequest{
		Bucket: bucket,
	})
	if err != nil {
		writeError(w, err)
		return
	}
	writeResponse(w, nil)
}

func (s *ProxyServer) putObject(w http.ResponseWriter, r *http.Request) {
	p, err := checkParameter(r, []string{"bucket", "key", "tag", "token"})
	if err != nil {
		writeError(w, err)
		return
	}
	bucket, key, rtag, token := p["bucket"], p["key"], p["tag"], p["token"]
	_, err = s.authClient.Check(context.Background(), &pa.CheckRequest{
		Token:      token,
		Bucket:     bucket,
		Permission: global.PermissionWrite,
	})
	if err != nil {
		writeError(w, err)
		return
	}
	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		writeError(w, err)
		return
	}
	ctx := context.Background()
	tag := fmt.Sprintf("%x", sha256.Sum256(body))
	if tag != rtag {
		writeError(w, status.Error(codes.Unknown, "data transmission error"))
		return
	}
	response, err := s.metaClient.CheckMeta(ctx, &pm.CheckMetaRequest{
		Bucket: bucket,
		Key:    key,
		Tag:    tag,
	})
	if err != nil {
		writeError(w, err)
		return
	}
	if response.Existed {
		writeResponse(w, nil)
		return
	}
	address := response.Address
	s.m.Lock()
	connection, ok := s.dataClients[address]
	if !ok {
		connection, err = grpc.Dial(address, grpc.WithInsecure())
		if err != nil {
			writeError(w, err)
			s.m.Unlock()
			return
		}
		if len(s.dataClients) >= maxStorageConnection {
			for k := range s.dataClients {
				s.dataClients[k].Close()
				delete(s.dataClients, k)
				s.dataClients[address] = connection
				break
			}
		}
	}
	s.m.Unlock()
	storageClient := ps.NewStorageForProxyClient(connection)
	ctx = context.Background()
	putResponse, err := storageClient.Put(ctx, &ps.PutRequest{
		Body: string(body),
		Tag:  tag,
	})
	if err != nil {
		writeError(w, err)
		return
	}
	_, err = s.metaClient.PutMeta(ctx, &pm.PutMetaRequest{
		Bucket:   bucket,
		Key:      key,
		Tag:      tag,
		Address:  address,
		VolumeId: putResponse.VolumeId,
		Offset:   putResponse.Offset,
		Size:     int64(len(body)),
	})
	if err != nil {
		writeError(w, err)
		return
	}
	writeResponse(w, nil)
}

func (s *ProxyServer) getObject(w http.ResponseWriter, r *http.Request) {
	p, err := checkParameter(r, []string{"bucket", "key", "token"})
	if err != nil {
		writeError(w, err)
		return
	}
	bucket, key, token := p["bucket"], p["key"], p["token"]
	_, err = s.authClient.Check(context.Background(), &pa.CheckRequest{
		Token:      token,
		Bucket:     bucket,
		Permission: global.PermissionRead,
	})
	if err != nil {
		writeError(w, err)
		return
	}
	ctx := context.Background()
	getMetaResponse, err := s.metaClient.GetMeta(ctx, &pm.GetMetaRequest{
		Bucket: bucket,
		Key:    key,
	})
	if err != nil {
		writeError(w, err)
		return
	}
	address := getMetaResponse.Address
	s.m.Lock()
	connection, ok := s.dataClients[address]
	if !ok {
		connection, err = grpc.Dial(address, grpc.WithInsecure())
		if err != nil {
			writeError(w, err)
			s.m.Unlock()
			return
		}
		if len(s.dataClients) >= maxStorageConnection {
			for k := range s.dataClients {
				s.dataClients[k].Close()
				delete(s.dataClients, k)
				s.dataClients[address] = connection
				break
			}
		}
	}
	s.m.Unlock()
	storageClient := ps.NewStorageForProxyClient(connection)
	ctx = context.Background()
	getResponse, err := storageClient.Get(ctx, &ps.GetRequest{
		VolumeId: getMetaResponse.VolumeId,
		Offset:   getMetaResponse.Offset,
	})
	if err != nil {
		writeError(w, err)
		return
	}
	writeResponse(w, []byte(getResponse.Body))
}

func (s *ProxyServer) deleteObject(w http.ResponseWriter, r *http.Request) {
	p, err := checkParameter(r, []string{"bucket", "key", "token"})
	if err != nil {
		writeError(w, err)
		return
	}
	bucket, key, token := p["bucket"], p["key"], p["token"]
	_, err = s.authClient.Check(context.Background(), &pa.CheckRequest{
		Token:      token,
		Bucket:     bucket,
		Permission: global.PermissionWrite,
	})
	if err != nil {
		writeError(w, err)
		return
	}
	_, err = s.metaClient.DeleteMeta(context.Background(), &pm.DeleteMetaRequest{
		Bucket: bucket,
		Key:    key,
	})
	if err != nil {
		writeError(w, err)
		return
	}
	writeResponse(w, nil)
}

func (s *ProxyServer) getObjectMeta(w http.ResponseWriter, r *http.Request) {
	p, err := checkParameter(r, []string{"bucket", "key", "token"})
	if err != nil {
		writeError(w, err)
		return
	}
	bucket, key, token := p["bucket"], p["key"], p["token"]
	_, err = s.authClient.Check(context.Background(), &pa.CheckRequest{
		Token:      token,
		Bucket:     bucket,
		Permission: global.PermissionRead,
	})
	if err != nil {
		writeError(w, err)
		return
	}
	response, err := s.metaClient.GetMeta(context.Background(), &pm.GetMetaRequest{
		Bucket: bucket,
		Key:    key,
	})
	if err != nil {
		writeError(w, err)
		return
	}
	writeResponse(w, []byte(response.String()))
}

func (s *ProxyServer) loginUser(w http.ResponseWriter, r *http.Request) {
	p, err := checkParameter(r, []string{"name", "pass"})
	if err != nil {
		writeError(w, err)
		return
	}
	name, pass := p["name"], p["pass"]
	response, err := s.authClient.Login(context.Background(), &pa.LoginRequest{
		Name: name,
		Pass: pass,
	})
	if err != nil {
		writeError(w, err)
	}
	writeResponse(w, []byte(response.Token))
}

func (s *ProxyServer) grantUser(w http.ResponseWriter, r *http.Request) {
	p, err := checkParameter(r, []string{"name", "bucket", "permission", "token"})
	if err != nil {
		writeError(w, err)
		return
	}
	name, bucket, permission, token := p["name"], p["bucket"], p["permission"], p["token"]
	level, err := strconv.Atoi(permission)
	if err != nil {
		writeError(w, status.Error(codes.InvalidArgument, "permission should be a number"))
		return
	}
	_, err = s.authClient.Grant(context.Background(), &pa.GrantRequest{
		Token:      token,
		Name:       name,
		Bucket:     bucket,
		Permission: int64(level),
	})
	if err != nil {
		writeError(w, err)
		return
	}
	writeResponse(w, nil)
}

func (s *ProxyServer) createUser(w http.ResponseWriter, r *http.Request) {
	p, err := checkParameter(r, []string{"name", "pass", "role", "token"})
	if err != nil {
		writeError(w, err)
		return
	}
	name, pass, role, token := p["name"], p["pass"], p["role"], p["token"]
	number, err := strconv.Atoi(role)
	if err != nil {
		writeError(w, status.Error(codes.InvalidArgument, "role should be a number"))
		return
	}
	_, err = s.authClient.Register(context.Background(), &pa.RegisterRequest{
		Token: token,
		Name:  name,
		Pass:  pass,
		Role:  int64(number),
	})
	if err != nil {
		writeError(w, err)
	}
	writeResponse(w, nil)
}
