package proxy

import (
	"context"
	"io/ioutil"
	"math/rand"
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
	if len(r.URL.Query()["key"]) == 0 {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	key := r.URL.Query()["key"][0]
	if len(key) == 0 {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(err.Error()))
		return
	}
	ctx := context.Background()
	response, err := s.metadataClient.ListStorage(ctx, &pm.ListStorageRequest{})
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(err.Error()))
		return
	}
	address := response.Address[rand.Intn(len(response.Address))]
	connection, ok := s.storageClients[address]
	if !ok {
		connection, err = grpc.Dial(address, grpc.WithInsecure())
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			_, _ = w.Write([]byte(err.Error()))
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
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(err.Error()))
		return
	}
	w.WriteHeader(http.StatusOK)
}

func (s *ProxyServer) get(w http.ResponseWriter, r *http.Request) {
	if len(r.URL.Query()["key"]) == 0 {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	key := r.URL.Query()["key"][0]
	if len(key) == 0 {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	ctx := context.Background()
	locateResponse, err := s.metadataClient.Locate(ctx, &pm.LocateRequest{
		Key: key,
	})
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(err.Error()))
		return
	}
	address := locateResponse.Address
	connection, ok := s.storageClients[address]
	if !ok {
		connection, err = grpc.Dial(address, grpc.WithInsecure())
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			_, _ = w.Write([]byte(err.Error()))
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
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(err.Error()))
		return
	}
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(getResponse.Body))
}
