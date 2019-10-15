package metadata

import (
	"context"
	"os"
	pm "oss/proto/metadata"
	ps "oss/proto/storage"

	"github.com/sirupsen/logrus"
	"google.golang.org/grpc"
)

type MetadataServer struct {
	pm.MetadataForStorageServer
	pm.MetadataForProxyServer
	address        string
	storageClients map[string]ps.StorageForMetadataClient
}

func NewMetadataServer(address string) *MetadataServer {
	return &MetadataServer{
		address:        address,
		storageClients: make(map[string]ps.StorageForMetadataClient),
	}
}

func (s *MetadataServer) Heartbeat(ctx context.Context, request *pm.HeartbeatRequest) (*pm.HeartbeatResponse, error) {
	address := request.Address
	_, ok := s.storageClients[address]
	if !ok {
		connection, err := grpc.Dial(request.Address, grpc.WithInsecure())
		if err != nil {
			return nil, err
		}
		storageClient := ps.NewStorageForMetadataClient(connection)
		s.storageClients[address] = storageClient
		logrus.WithField("address", address).Info("Connect to new storage server")
	}
	return &pm.HeartbeatResponse{}, nil
}

func (s *MetadataServer) Locate(ctx context.Context, request *pm.LocateRequest) (*pm.LocateResponse, error) {
	key := request.Key
	for address, client := range s.storageClients {
		_, err := client.Locate(ctx, &ps.LocateRequest{
			Key: key,
		})
		if err == nil {
			return &pm.LocateResponse{
				Address: address,
			}, nil
		}
	}
	return nil, os.ErrNotExist
}

func (s *MetadataServer) ListStorage(ctx context.Context, request *pm.ListStorageRequest) (*pm.ListStorageResponse, error) {
	addresses := make([]string, 0)
	for address := range s.storageClients {
		addresses = append(addresses, address)
	}
	return &pm.ListStorageResponse{
		Address: addresses,
	}, nil
}
