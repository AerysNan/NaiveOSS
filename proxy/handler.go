package proxy

import (
	"context"
	"crypto/md5"
	"fmt"
	"io/ioutil"
	"net/http"

	pm "oss/proto/metadata"
	ps "oss/proto/storage"

	"github.com/sirupsen/logrus"
	"google.golang.org/grpc"
)

var maxStorageConnection = 10

type ProxyServer struct {
	http.Handler
	metadataClient pm.MetadataForProxyClient
	storageClients map[string]*grpc.ClientConn

	address string
}

func NewProxyServer(address string, metadataClient pm.MetadataForProxyClient) *ProxyServer {
	return &ProxyServer{
		address:        address,
		metadataClient: metadataClient,
		storageClients: make(map[string]*grpc.ClientConn),
	}
}

func (s *ProxyServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r == nil {
		return
	}
	switch r.Method {
	case http.MethodPost:
		logrus.Debug("Handle post method")
		s.post(w, r)
	case http.MethodPut:
		logrus.Debug("Handle put method")
		s.put(w, r)
	case http.MethodGet:
		logrus.Debug("Handle get method")
		s.get(w, r)
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

func (s *ProxyServer) post(w http.ResponseWriter, r *http.Request) {
	p, err := checkParameter(r, []string{"bucket"})
	if err != nil {
		writeError(w, err)
		return
	}
	bucket := p[0]
	_, err = s.metadataClient.CreateBucket(context.Background(), &pm.CreateBucketRequest{
		Bucket: bucket,
	})
	if err != nil {
		writeError(w, err)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func (s *ProxyServer) put(w http.ResponseWriter, r *http.Request) {
	p, err := checkParameter(r, []string{"bucket", "key"})
	if err != nil {
		writeError(w, err)
		return
	}
	bucket, key := p[0], p[1]
	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		writeError(w, err)
		return
	}
	ctx := context.Background()
	tag := fmt.Sprintf("%x", md5.Sum([]byte(body)))
	response, err := s.metadataClient.CheckMeta(ctx, &pm.CheckMetaRequest{
		Bucket: bucket,
		Key:    key,
		Tag:    tag,
	})
	if err != nil {
		writeError(w, err)
		return
	}
	if response.Existed {
		w.WriteHeader(http.StatusOK)
		return
	}
	address := response.Address
	connection, ok := s.storageClients[address]
	if !ok {
		connection, err = grpc.Dial(address, grpc.WithInsecure())
		if err != nil {
			writeError(w, err)
			return
		}
		if len(s.storageClients) >= maxStorageConnection {
			for k := range s.storageClients {
				s.storageClients[k].Close()
				delete(s.storageClients, k)
				s.storageClients[address] = connection
				break
			}
		}
	}
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
	_, err = s.metadataClient.PutMeta(ctx, &pm.PutMetaRequest{
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
	w.WriteHeader(http.StatusOK)
}

func (s *ProxyServer) get(w http.ResponseWriter, r *http.Request) {
	p, err := checkParameter(r, []string{"bucket", "key"})
	if err != nil {
		writeError(w, err)
		return
	}
	bucket, key := p[0], p[1]
	ctx := context.Background()
	getMetaResponse, err := s.metadataClient.GetMeta(ctx, &pm.GetMetaRequest{
		Bucket: bucket,
		Key:    key,
	})
	if err != nil {
		writeError(w, err)
		return
	}
	address := getMetaResponse.Address
	connection, ok := s.storageClients[address]
	if !ok {
		connection, err = grpc.Dial(address, grpc.WithInsecure())
		if err != nil {
			writeError(w, err)
			return
		}
		if len(s.storageClients) >= maxStorageConnection {
			for k := range s.storageClients {
				s.storageClients[k].Close()
				delete(s.storageClients, k)
				s.storageClients[address] = connection
				break
			}
		}
	}
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
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(getResponse.Body))
}
