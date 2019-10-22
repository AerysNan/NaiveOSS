package storage

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"os"
	pm "oss/proto/metadata"
	ps "oss/proto/storage"
	"path"
	"strings"
	"time"

	"io/ioutil"

	"github.com/sirupsen/logrus"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var (
	VolumeMaxSize     = int64(1 << 10)
	HeartbeatInterval = 5 * time.Second
)

type Volume struct {
	volumeId int64
	size     int64
}

type StorageServer struct {
	ps.StorageForProxyServer
	ps.StorageForMetadataServer
	metadataClient pm.MetadataForStorageClient

	address string
	root    string

	currentVolume *Volume
	volumes       map[int64]*Volume
}

func NewStorageServer(address string, root string, metadataClient pm.MetadataForStorageClient) *StorageServer {
	storageServer := &StorageServer{
		metadataClient: metadataClient,
		address:        address,
		root:           root,
		volumes:        make(map[int64]*Volume),
		currentVolume:  new(Volume),
	}
	storageServer.recover()
	go storageServer.heartbeatLoop()
	return storageServer
}

func (s *StorageServer) recover() {
	files, err := ioutil.ReadDir(s.root)
	if err != nil {
		logrus.WithError(err).Error("Open recover directory failed")
		return
	}
	for _, file := range files {
		if strings.HasSuffix(file.Name(), ".dat") {
			err := s.recoverSingleFile(file.Name())
			if err != nil {
				logrus.WithError(err).Errorf("Recover from file %v failed", file.Name())
			}
		}
	}
}

func (s *StorageServer) recoverSingleFile(name string) error {
	file, err := os.Open(path.Join(s.root, name))
	if err != nil {
		return err
	}
	info, err := file.Stat()
	if err != nil {
		return err
	}
	var volumeId int64
	fmt.Sscanf(info.Name(), "%v.dat", &volumeId)
	s.volumes[volumeId] = &Volume{
		volumeId: volumeId,
		size:     info.Size(),
	}
	return nil
}

func (s *StorageServer) heartbeatLoop() {
	ticker := time.NewTicker(HeartbeatInterval)
	for {
		ctx := context.Background()
		_, err := s.metadataClient.Heartbeat(ctx, &pm.HeartbeatRequest{
			Address: s.address,
		})
		if err != nil {
			logrus.WithError(err).Error("Heartbeat failed")
		}
		<-ticker.C
	}
}

func (s *StorageServer) Get(ctx context.Context, request *ps.GetRequest) (*ps.GetResponse, error) {
	volumeId := request.VolumeId
	offset := request.Offset
	name := path.Join(s.root, fmt.Sprintf("%d.dat", volumeId))
	file, err := os.Open(name)
	if err != nil {
		logrus.WithError(err).Errorf("Open file %v failed", name)
		return nil, status.Error(codes.Internal, "Open data failed")
	}
	defer file.Close()
	bytes := make([]byte, 8)
	_, err = file.ReadAt(bytes, offset)
	if err != nil {
		logrus.WithError(err).Errorf("Read file %v failed", name)
		return nil, status.Error(codes.Internal, "read data failed")
	}
	data := make([]byte, int64(binary.BigEndian.Uint64(bytes)))
	_, err = file.ReadAt(data, offset+8)
	if err != nil {
		logrus.WithError(err).Errorf("Read file %v failed", name)
		return nil, status.Error(codes.Internal, "read data failed")
	}
	return &ps.GetResponse{
		Body: string(data),
	}, nil
}

func (s *StorageServer) Put(ctx context.Context, request *ps.PutRequest) (*ps.PutResponse, error) {
	data := request.Body
	tag := fmt.Sprintf("%x", sha256.Sum256([]byte(data)))
	if tag != request.Tag {
		return nil, status.Error(codes.Unauthenticated, "data operation not authenticated")
	}
	size := int64(len(data))
	name := path.Join(s.root, fmt.Sprintf("%d.dat", s.currentVolume.volumeId))
	file, err := os.OpenFile(name, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0766)
	if err != nil {
		logrus.WithError(err).Errorf("Open file %v failed", name)
		return nil, status.Error(codes.Internal, "open data failed")
	}
	defer file.Close()
	bytes := make([]byte, 8)
	binary.BigEndian.PutUint64(bytes, uint64(size))
	_, err = file.Write(bytes)
	if err != nil {
		logrus.WithError(err).Errorf("Write file %v failed", name)
		return nil, status.Error(codes.Internal, "write data failed")
	}
	_, err = file.Write([]byte(data))
	if err != nil {
		logrus.WithError(err).Errorf("Write file %v failed", name)
		return nil, status.Error(codes.Internal, "write data failed")
	}
	response := &ps.PutResponse{
		VolumeId: s.currentVolume.volumeId,
		Offset:   s.currentVolume.size,
	}
	s.currentVolume.size += 8 + size
	if s.currentVolume.size >= VolumeMaxSize {
		s.currentVolume.volumeId++
		s.currentVolume.size = 0
	}
	return response, nil
}
