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

func (s *ProxyServer) put(w http.ResponseWriter, r *http.Request) {
	p := checkParameter(w, r, []string{"bucket", "key"})
	if p == nil {
		return
	}
	bucket, key := p[0], p[1]
	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		writeError(w, err)
		return
	}
	ctx := context.Background()
	response, err := s.metadataClient.CheckMeta(ctx, &pm.CheckMetaRequest{
		Bucket: bucket,
		Key:    key,
		Tag:    fmt.Sprintf("%x", md5.Sum([]byte(body))),
	})
	if err != nil {
		writeError(w, err)
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
	_, err = storageClient.Put(ctx, &ps.PutRequest{
		Key:  key,
		Body: string(body),
	})
	// TODO: put meta to metadata server
	if err != nil {
		writeError(w, err)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func (s *ProxyServer) get(w http.ResponseWriter, r *http.Request) {
	p := checkParameter(w, r, []string{"bucket", "key"})
	if p == nil {
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
		Key: key,
	})
	if err != nil {
		writeError(w, err)
		return
	}
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(getResponse.Body))
}
