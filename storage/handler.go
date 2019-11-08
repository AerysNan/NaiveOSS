package storage

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"sync"
	"time"

	pm "oss/proto/metadata"
	ps "oss/proto/storage"

	"github.com/sirupsen/logrus"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type Config struct {
	VolumeMaxSize    int64
	DumpFileName     string
	DumpTimeout      time.Duration
	HeartbeatTimeout time.Duration
}

type Volume struct {
	m sync.RWMutex

	ID      int64
	Size    int64
	Content int64
}

func NewVolume(id int64) *Volume {
	return &Volume{
		ID:      id,
		Size:    0,
		Content: 0,

		m: sync.RWMutex{},
	}
}

type StorageServer struct {
	ps.StorageForProxyServer
	ps.StorageForMetadataServer
	metadataClient pm.MetadataForStorageClient

	address string
	root    string
	config  *Config
	m       sync.RWMutex

	Volumes       map[int64]*Volume
	CurrentVolume int64
}

func NewStorageServer(address string, root string, metadataClient pm.MetadataForStorageClient, config *Config) *StorageServer {
	storageServer := &StorageServer{
		metadataClient: metadataClient,
		address:        address,
		config:         config,
		root:           root,
		m:              sync.RWMutex{},

		Volumes:       make(map[int64]*Volume),
		CurrentVolume: 0,
	}
	storageServer.recover()
	storageServer.addVolume()
	go storageServer.dumpLoop()
	go storageServer.heartbeatLoop()
	return storageServer
}

func (s *StorageServer) recover() {
	filePath := path.Join(s.root, s.config.DumpFileName)
	_, err := os.Stat(filePath)
	if os.IsNotExist(err) {
		return
	}
	file, err := os.Open(filePath)
	if err != nil {
		logrus.WithError(err).Error("Open dump file failed")
		return
	}
	defer file.Close()
	bytes, err := ioutil.ReadAll(file)
	if err != nil {
		logrus.WithError(err).Error("Read dump file failed")
		return
	}
	err = json.Unmarshal(bytes, s)
	if err != nil {
		logrus.WithError(err).Error("Unmarshal JSON failed")
		return
	}
}

func (s *StorageServer) heartbeatLoop() {
	ticker := time.NewTicker(s.config.HeartbeatTimeout)
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

func (s *StorageServer) dumpLoop() {
	ticker := time.NewTicker(s.config.DumpTimeout)
	for {
		func() {
			s.m.RLock()
			bytes, err := json.Marshal(s)
			s.m.RUnlock()
			if err != nil {
				logrus.WithError(err).Error("Marshal JSON failed")
				return
			}
			file, err := os.OpenFile(path.Join(s.root, s.config.DumpFileName), os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0766)
			if err != nil {
				logrus.WithError(err).Error("Open dump file falied")
				return
			}
			defer file.Close()
			_, err = file.Write(bytes)
			if err != nil {
				logrus.WithError(err).Error("Write dump file failed")
			}
		}()
		<-ticker.C
	}
}

func (s *StorageServer) State(ctx context.Context, request *ps.StateRequest) (*ps.StateResponse, error) {
	s.m.RLock()
	defer s.m.RUnlock()
	id := request.VolumeId
	volume, ok := s.Volumes[id]
	if !ok {
		return nil, status.Error(codes.NotFound, "no such volume")
	}
	return &ps.StateResponse{
		Size: volume.Content,
	}, nil
}

func (s *StorageServer) Migrate(ctx context.Context, request *ps.MigrateRequest) (*ps.MigrateResponse, error) {
	sourceID, sourceOffset := request.VolumeId, request.Offset
	s.m.RLock()
	sourceVolume, ok := s.Volumes[sourceID]
	if !ok {
		s.m.RUnlock()
		return nil, status.Error(codes.NotFound, "no such volume")
	}
	sourceVolume.m.RLock()
	s.m.RUnlock()
	name := path.Join(s.root, fmt.Sprintf("%d.dat", sourceID))
	file, err := os.Open(name)
	if err != nil {
		logrus.WithError(err).Errorf("Open file %v failed", name)
		return nil, status.Error(codes.Internal, "open data failed")
	}
	bytes := make([]byte, 8)
	_, err = file.ReadAt(bytes, sourceOffset)
	if err != nil {
		logrus.WithError(err).Errorf("Read file %v failed", name)
		return nil, status.Error(codes.Internal, "read data failed")
	}
	size := int64(binary.BigEndian.Uint64(bytes))
	data := make([]byte, size)
	_, err = file.ReadAt(data, sourceOffset+8)
	if err != nil {
		logrus.WithError(err).Errorf("Read file %v failed", name)
		return nil, status.Error(codes.Internal, "read data failed")
	}
	file.Close()
	sourceVolume.m.RUnlock()

	s.m.RLock()
	name = path.Join(s.root, fmt.Sprintf("%d.dat", s.CurrentVolume))
	currentID := s.CurrentVolume
	targetVolume, ok := s.Volumes[currentID]
	if !ok {
		s.m.RUnlock()
		return nil, status.Error(codes.NotFound, "no such volume")
	}
	targetVolume.m.Lock()
	defer targetVolume.m.Unlock()
	s.m.RUnlock()
	file, err = os.OpenFile(name, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0766)
	if err != nil {
		logrus.WithError(err).Errorf("Open file %v failed", name)
		return nil, status.Error(codes.Internal, "open data failed")
	}
	defer file.Close()
	bytes = make([]byte, 8)
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
	response := &ps.MigrateResponse{
		VolumeId: targetVolume.ID,
		Offset:   targetVolume.Size,
	}
	targetVolume.Size += 8 + size
	targetVolume.Content += size
	if targetVolume.Size >= s.config.VolumeMaxSize {
		s.addVolume()
	}
	return response, nil
}

func (s *StorageServer) Rotate(ctx context.Context, request *ps.RotateRequest) (*ps.RotateResponse, error) {
	s.addVolume()
	return &ps.RotateResponse{}, nil
}

func (s *StorageServer) DeleteVolume(ctx context.Context, request *ps.DeleteVolumeRequest) (*ps.DeleteVolumeResponse, error) {
	volumeID := request.VolumeId
	if volumeID == s.CurrentVolume {
		return nil, status.Error(codes.FailedPrecondition, "cannot delete current volume")
	}
	err := os.Remove(path.Join(s.root, fmt.Sprintf("%d.dat", volumeID)))
	if err != nil {
		logrus.WithError(err).Warnf("Delete volume %v failed", volumeID)
	}
	return &ps.DeleteVolumeResponse{}, nil
}

func (s *StorageServer) Get(ctx context.Context, request *ps.GetRequest) (*ps.GetResponse, error) {
	id := request.VolumeId
	offset := request.Offset
	name := path.Join(s.root, fmt.Sprintf("%d.dat", id))
	s.m.RLock()
	volume, ok := s.Volumes[id]
	if !ok {
		s.m.RUnlock()
		return nil, status.Error(codes.NotFound, "no such volume")
	}
	volume.m.RLock()
	defer volume.m.RUnlock()
	s.m.RUnlock()
	file, err := os.Open(name)
	if err != nil {
		logrus.WithError(err).Errorf("Open file %v failed", name)
		return nil, status.Error(codes.Internal, "open data failed")
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
	s.m.RLock()
	name := path.Join(s.root, fmt.Sprintf("%d.dat", s.CurrentVolume))
	currentID := s.CurrentVolume
	volume, ok := s.Volumes[currentID]
	if !ok {
		s.m.RUnlock()
		return nil, status.Error(codes.NotFound, "no such volume")
	}
	volume.m.Lock()
	defer volume.m.Unlock()
	s.m.RUnlock()
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
		VolumeId: volume.ID,
		Offset:   volume.Size,
	}
	volume.Size += 8 + size
	volume.Content += size
	if volume.Size >= s.config.VolumeMaxSize {
		s.addVolume()
	}
	return response, nil
}

func (s *StorageServer) addVolume() {
	s.m.Lock()
	defer s.m.Unlock()
	logrus.Debugf("Volume index increase from %v to %v", s.CurrentVolume, s.CurrentVolume+1)
	s.CurrentVolume++
	s.Volumes[s.CurrentVolume] = NewVolume(s.CurrentVolume)
}
