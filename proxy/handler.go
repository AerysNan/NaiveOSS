package proxy

import (
	"context"
	"fmt"
	"io/ioutil"
	"net/http"
	_ "net/http/pprof"
	"strconv"
	"strings"
	"sync"
	"time"

	"oss/global"
	pa "oss/proto/auth"
	pm "oss/proto/metadata"
	ps "oss/proto/storage"

	"github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const maxStorageConnection = 10
const checkExpiredBlobsTimeout = 360000000000

type Server struct {
	http.Handler
	authClient  pa.AuthForProxyClient
	metaClient  pm.MetadataForProxyClient
	dataClients map[string]*grpc.ClientConn
	m           *sync.RWMutex
	UploadTask  map[string]string
	Address     string
}

func NewProxyServer(address string, authClient pa.AuthForProxyClient, metadataClient pm.MetadataForProxyClient) *Server {
	s := &Server{
		Address:     address,
		UploadTask:  make(map[string]string),
		m:           new(sync.RWMutex),
		authClient:  authClient,
		metaClient:  metadataClient,
		dataClients: make(map[string]*grpc.ClientConn),
	}
	go s.checkExpiredBlobs()
	return s
}

func (s *Server) checkExpiredBlobs() {
	ticker := time.NewTicker(checkExpiredBlobsTimeout)
	for {
		s.m.RLock()
		conns := s.dataClients
		s.m.RUnlock()
		blobs := make([]string, 0)
		for address, conn := range conns {
			client := ps.NewStorageForProxyClient(conn)
			response, err := client.CheckBlob(context.Background(), &ps.CheckBlobRequest{})
			if err != nil {
				logrus.WithField("address", address).Warn("connection fail")
			}
			blobs = append(blobs, response.Id...)
		}
		s.m.Lock()
		for _, id := range blobs {
			delete(s.UploadTask, id)
		}
		s.m.Unlock()
		<-ticker.C
	}
}

func (s *Server) createBucket(w http.ResponseWriter, r *http.Request) {
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

func (s *Server) listBucket(w http.ResponseWriter, r *http.Request) {
	_, err := checkParameter(r, []string{"token"})
	if err != nil {
		writeError(w, err)
		return
	}
	response, err := s.metaClient.ListBucket(context.Background(), &pm.ListBucketRequest{})
	if err != nil {
		writeError(w, err)
		return
	}
	writeResponse(w, []byte(strings.Join(response.Buckets, " ")))
}

func (s *Server) deleteBucket(w http.ResponseWriter, r *http.Request) {
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

func (s *Server) createUploadID(w http.ResponseWriter, r *http.Request) {
	p, err := checkParameter(r, []string{"bucket", "name", "key", "tag", "token", "id"})
	if err != nil {
		writeError(w, err)
		return
	}
	bucket, name, key, tag, token, id := p["bucket"], p["name"], p["key"], p["tag"], p["token"], p["id"]
	ctx := context.Background()
	_, err = s.authClient.Check(ctx, &pa.CheckRequest{
		Token:      token,
		Bucket:     bucket,
		Permission: global.PermissionWrite,
	})
	if err != nil {
		writeError(w, err)
		return
	}
	response, err := s.metaClient.CheckMeta(ctx, &pm.CheckMetaRequest{
		Bucket: bucket,
		Name:   name,
		Key:    key,
		Tag:    tag,
	})
	if err != nil {
		writeError(w, err)
		return
	}
	if response.Existed {
		writeResponse(w, []byte(strconv.Itoa(0)))
		return
	}
	if id != "0" {
		s.m.RLock()
		_, ok := s.UploadTask[id]
		s.m.RUnlock()
		if ok {
			writeResponse(w, []byte(id))
			return
		}
	}
	address := response.Address
	s.m.Lock()
	id = strconv.FormatInt(time.Now().UnixNano(), 10)
	s.UploadTask[id] = address
	s.m.Unlock()
	connection, err := s.validateConnection(address)
	if err != nil {
		writeError(w, err)
		return
	}
	storageClient := ps.NewStorageForProxyClient(connection)
	_, err = storageClient.Create(ctx, &ps.CreateRequest{
		Tag: tag,
		Id:  id,
	})
	if err != nil {
		writeError(w, err)
		return
	}
	writeResponse(w, []byte(id))
}

func (s *Server) confirmUploadID(w http.ResponseWriter, r *http.Request) {
	p, err := checkParameter(r, []string{"id", "name", "bucket", "key", "tag", "token"})
	if err != nil {
		writeError(w, err)
		return
	}
	id, name, key, bucket, tag, token := p["id"], p["name"], p["key"], p["bucket"], p["tag"], p["token"]
	ctx := context.Background()
	_, err = s.authClient.Check(ctx, &pa.CheckRequest{
		Token:      token,
		Bucket:     bucket,
		Permission: global.PermissionWrite,
	})
	if err != nil {
		writeError(w, err)
		return
	}
	address, ok := s.UploadTask[id]
	if !ok {
		logrus.Warnf("invalid id %v\n", id)
		writeError(w, status.Error(codes.InvalidArgument, "invalid upload id value"))
		return
	}
	connection, err := s.validateConnection(address)
	if err != nil {
		writeError(w, err)
		return
	}
	storageClient := ps.NewStorageForProxyClient(connection)
	confirmResponse, err := storageClient.Confirm(ctx, &ps.ConfirmRequest{
		Id: id,
	})
	if err != nil {
		writeError(w, err)
		return
	}
	_, err = s.metaClient.PutMeta(ctx, &pm.PutMetaRequest{
		Bucket:   bucket,
		Key:      key,
		Tag:      tag,
		Name:     name,
		Address:  address,
		VolumeId: confirmResponse.VolumeId,
		Offset:   confirmResponse.Offset,
		Size:     confirmResponse.Size,
	})
	if err != nil {
		writeError(w, err)
		return
	}
	writeResponse(w, nil)
}

func (s *Server) putObject(w http.ResponseWriter, r *http.Request) {
	p, err := checkParameter(r, []string{"id", "bucket", "offset", "token"})
	if err != nil {
		writeError(w, err)
		return
	}
	id, offsetS, bucket, token := p["id"], p["offset"], p["bucket"], p["token"]
	ctx := context.Background()
	_, err = s.authClient.Check(ctx, &pa.CheckRequest{
		Token:      token,
		Bucket:     bucket,
		Permission: global.PermissionWrite,
	})
	if err != nil {
		writeError(w, err)
		return
	}
	offset, err := strconv.ParseInt(offsetS, 10, 64)
	if err != nil {
		writeError(w, status.Error(codes.InvalidArgument, "invalid upload file offset"))
		return
	}
	address, ok := s.UploadTask[id]
	if !ok {
		logrus.Warnf("invalid id %v\n", id)
		writeError(w, status.Error(codes.InvalidArgument, "invalid upload id value"))
		return
	}
	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		writeError(w, err)
		return
	}
	connection, err := s.validateConnection(address)
	if err != nil {
		writeError(w, err)
		return
	}
	storageClient := ps.NewStorageForProxyClient(connection)
	_, err = storageClient.Put(ctx, &ps.PutRequest{
		Body:   body,
		Id:     id,
		Offset: offset,
	})
	if err != nil {
		writeError(w, err)
		return
	}
	writeResponse(w, nil)
}

func (s *Server) getObject(w http.ResponseWriter, r *http.Request) {
	p, err := checkParameter(r, []string{"bucket", "key", "token", "start"})
	if err != nil {
		writeError(w, err)
		return
	}
	bucket, key, token, startS := p["bucket"], p["key"], p["token"], p["start"]
	start, _ := strconv.ParseInt(startS, 10, 64)
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
	connection, err := s.validateConnection(address)
	if err != nil {
		writeError(w, err)
		return
	}
	storageClient := ps.NewStorageForProxyClient(connection)
	getResponse, err := storageClient.Get(ctx, &ps.GetRequest{
		VolumeId: getMetaResponse.VolumeId,
		Offset:   getMetaResponse.Offset,
		Start:    start,
	})
	if err != nil {
		writeError(w, err)
		return
	}
	w.Header().Add("name", getMetaResponse.Name)
	writeResponse(w, []byte(getResponse.Body))
}

func (s *Server) deleteObject(w http.ResponseWriter, r *http.Request) {
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

func (s *Server) getObjectMeta(w http.ResponseWriter, r *http.Request) {
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

func (s *Server) rangeObject(w http.ResponseWriter, r *http.Request) {
	p, err := checkParameter(r, []string{"bucket", "from", "to", "token"})
	if err != nil {
		writeError(w, err)
		return
	}
	bucket, from, to, token := p["bucket"], p["from"], p["to"], p["token"]
	_, err = s.authClient.Check(context.Background(), &pa.CheckRequest{
		Token:      token,
		Bucket:     bucket,
		Permission: global.PermissionRead,
	})
	if err != nil {
		writeError(w, err)
		return
	}
	response, err := s.metaClient.RangeObject(context.Background(), &pm.RangeObjectRequest{
		Bucket: bucket,
		From:   from,
		To:     to,
	})
	if err != nil {
		writeError(w, err)
		return
	}
	writeResponse(w, []byte(fmt.Sprintf("%v", response.Key)))
}

func (s *Server) listObject(w http.ResponseWriter, r *http.Request) {
	p, err := checkParameter(r, []string{"bucket", "token"})
	if err != nil {
		writeError(w, err)
		return
	}
	bucket, token := p["bucket"], p["token"]
	_, err = s.authClient.Check(context.Background(), &pa.CheckRequest{
		Token:      token,
		Bucket:     bucket,
		Permission: global.PermissionRead,
	})
	if err != nil {
		writeError(w, err)
		return
	}
	response, err := s.metaClient.ListObject(context.Background(), &pm.ListObjectRequest{
		Bucket: bucket,
	})
	if err != nil {
		writeError(w, err)
		return
	}
	objects := make([]string, 0)
	for _, object := range response.Objects {
		objects = append(objects, fmt.Sprintf("%v %v %v %v\n", object.Key, object.Name, object.Size, time.Unix(object.CreatedTime, 0).Format(time.RFC3339)))
	}
	writeResponse(w, []byte(strings.Join(objects, "")))
}

func (s *Server) loginUser(w http.ResponseWriter, r *http.Request) {
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
		return
	}
	writeResponse(w, []byte(response.Token))
}

func (s *Server) grantUser(w http.ResponseWriter, r *http.Request) {
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

func (s *Server) createUser(w http.ResponseWriter, r *http.Request) {
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
		return
	}
	writeResponse(w, nil)
}

func (s *Server) validateConnection(address string) (*grpc.ClientConn, error) {
	s.m.RLock()
	connection, ok := s.dataClients[address]
	s.m.RUnlock()
	var err error
	if !ok {
		diaOpt := grpc.WithDefaultCallOptions(grpc.MaxCallSendMsgSize(global.MaxTransportSize), grpc.MaxCallRecvMsgSize(global.MaxTransportSize))
		connection, err = grpc.Dial(address, grpc.WithInsecure(), diaOpt)
		if err != nil {
			return nil, err
		}
		s.m.Lock()
		if len(s.dataClients) >= maxStorageConnection {
			for k := range s.dataClients {
				s.dataClients[k].Close()
				delete(s.dataClients, k)
				break
			}
		}
		s.dataClients[address] = connection
		s.m.Unlock()
	}
	return connection, nil
}
