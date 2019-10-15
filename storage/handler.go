package storage

import (
	"context"
	"io/ioutil"
	"os"
	pm "oss/proto/metadata"
	ps "oss/proto/storage"
	"path"
	"time"

	"github.com/sirupsen/logrus"
)

type StorageServer struct {
	ps.StorageForProxyServer
	ps.StorageForMetadataServer
	metadataClient pm.MetadataForStorageClient

	address string
	root    string
}

func NewStorageServer(address string, root string, metadataClient pm.MetadataForStorageClient) *StorageServer {
	storageServer := &StorageServer{
		metadataClient: metadataClient,
		address:        address,
		root:           root,
	}
	go storageServer.heartbeatLoop()
	return storageServer
}

func (s *StorageServer) heartbeatLoop() {
	ticker := time.NewTicker(5 * time.Second)
	for {
		<-ticker.C
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		_, err := s.metadataClient.Heartbeat(ctx, &pm.HeartbeatRequest{
			Address: s.address,
		})
		if err != nil {
			logrus.WithError(err).Warn("Heartbeat failed")
		}
		cancel()
	}
}

func (s *StorageServer) Locate(ctx context.Context, request *ps.LocateRequest) (*ps.LocateResponse, error) {
	key := request.Key
	_, err := os.Stat(path.Join(s.root, key))
	if err != nil {
		return nil, err
	}
	return &ps.LocateResponse{}, nil
}

func (s *StorageServer) Get(ctx context.Context, request *ps.GetRequest) (*ps.GetResponse, error) {
	key := request.Key
	file, err := os.Open(path.Join(s.root, key))
	if err != nil {
		return nil, err
	}
	bytes, err := ioutil.ReadAll(file)
	if err != nil {
		return nil, err
	}
	return &ps.GetResponse{
		Body: string(bytes),
	}, nil
}

func (s *StorageServer) Put(ctx context.Context, request *ps.PutRequest) (*ps.PutResponse, error) {
	key := request.Key
	file, err := os.Create(path.Join(s.root, key))
	if err != nil {
		return nil, err
	}
	_, err = file.WriteString(request.Body)
	if err != nil {
		return nil, err
	}
	return &ps.PutResponse{}, nil
}
