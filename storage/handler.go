package storage

import (
	"context"
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
)

var (
	VolumeMaxSize       int64 = 10
	connectLoopInterval       = 5 * time.Second
	connectTimeout            = 2 * time.Second
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
	go storageServer.connectToMetaServer()
	return storageServer
}

func (s *StorageServer) connectToMetaServer() {
	ticker := time.NewTicker(connectLoopInterval)
	for {
		<-ticker.C
		ctx, cancel := context.WithTimeout(context.Background(), connectTimeout)
		_, err := s.metadataClient.Register(ctx, &pm.RegisterRequest{
			Address: s.address,
		})
		cancel()
		if err != nil {
			logrus.WithError(err).Error("Connect to metadata server failed")
		} else {
			return
		}
	}
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

func (s *StorageServer) Get(ctx context.Context, request *ps.GetRequest) (*ps.GetResponse, error) {
	volumeId := request.VolumeId
	offset := request.Offset
	file, err := os.Open(path.Join(s.root, fmt.Sprintf("%d", volumeId)+".dat"))
	if err != nil {
		return nil, err
	}
	defer file.Close()
	bytes := make([]byte, 8)
	_, err = file.ReadAt(bytes, offset)
	if err != nil {
		return nil, err
	}
	data := make([]byte, int64(binary.BigEndian.Uint64(bytes)))
	_, err = file.ReadAt(data, offset+8)
	if err != nil {
		return nil, err
	}
	return &ps.GetResponse{
		Body: string(data),
	}, nil
}

func (s *StorageServer) Put(ctx context.Context, request *ps.PutRequest) (*ps.PutResponse, error) {
	data := request.Body
	size := int64(len(data))
<<<<<<< Updated upstream
	file, err := os.OpenFile(path.Join(s.root, fmt.Sprintf("%d", s.currentVolume.volumeId)+".dat"), os.O_APPEND|os.O_CREATE, 0766)
=======
	name := path.Join(s.root, fmt.Sprintf("%d.dat", s.currentVolume.volumeId))
	file, err := os.OpenFile(name, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0766)
>>>>>>> Stashed changes
	if err != nil {
		return nil, err
	}
	defer file.Close()
	bytes := make([]byte, 8)
	binary.BigEndian.PutUint64(bytes, uint64(size))
	_, err = file.Write(bytes)
	if err != nil {
		return nil, err
	}
	_, err = file.Write([]byte(data))
	if err != nil {
		return nil, err
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
