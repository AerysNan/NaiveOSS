package storage

import (
	"bytes"
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

	"github.com/klauspost/reedsolomon"
	"github.com/natefinch/atomic"
	"github.com/sirupsen/logrus"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var enc reedsolomon.Encoder

type Config struct {
	VolumeMaxSize    int64
	DumpFileName     string
	DataShard        int
	ParityShard      int
	BSDNum           int
	DumpTimeout      time.Duration
	HeartbeatTimeout time.Duration
}

type StorageServer struct {
	ps.StorageForProxyServer
	ps.StorageForMetadataServer
	metadataClient pm.MetadataForStorageClient

	m             sync.RWMutex
	config        *Config
	Address       string `json:"-"`
	Root          string `json:"-"`
	BSDs          map[int64]*BSD
	Volumes       map[int64]*Volume
	CurrentVolume int64
}

func NewStorageServer(address string, root string, metadataClient pm.MetadataForStorageClient, config *Config) *StorageServer {
	encoderInit(config)
	storageServer := &StorageServer{
		metadataClient: metadataClient,
		m:              sync.RWMutex{},
		config:         config,
		Address:        address,
		Root:           root,
		BSDs:           make(map[int64]*BSD),
		Volumes:        make(map[int64]*Volume),
		CurrentVolume:  0,
	}
	storageServer.recover()
	storageServer.bsdInit()
	storageServer.addVolume()
	go storageServer.dumpLoop()
	go storageServer.heartbeatLoop()
	return storageServer
}

func (s *StorageServer) recover() {
	filePath := path.Join(s.Root, s.config.DumpFileName)
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

func (s *StorageServer) bsdInit() {
	if len(s.BSDs) != s.config.BSDNum {
		for i := 0; i < s.config.BSDNum; i++ {
			path := path.Join(s.Root, fmt.Sprintf("BSD_%d", i))
			err := os.Mkdir(path, os.ModePerm)
			if err != nil && !os.IsExist(err) {
				logrus.WithError(err).Errorf("%s init failed", path)
			} else {
				s.BSDs[int64(i)] = NewBSD(path)
			}
		}
	}
}

func (s *StorageServer) heartbeatLoop() {
	ticker := time.NewTicker(s.config.HeartbeatTimeout)
	for {
		ctx := context.Background()
		_, err := s.metadataClient.Heartbeat(ctx, &pm.HeartbeatRequest{
			Address: s.Address,
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
			data, err := json.Marshal(s)
			s.m.RUnlock()
			if err != nil {
				logrus.WithError(err).Error("Marshal JSON failed")
				return
			}
			err = atomic.WriteFile(path.Join(s.Root, s.config.DumpFileName), bytes.NewReader(data))
			if err != nil {
				logrus.WithError(err).Error("Write dump file failed")
			}
		}()
		<-ticker.C
	}
}

func (s *StorageServer) State(ctx context.Context, request *ps.StateRequest) (*ps.StateResponse, error) {
	id := request.VolumeId
	s.m.RLock()
	defer s.m.RUnlock()
	volume, ok := s.Volumes[id]
	if !ok {
		return nil, status.Error(codes.NotFound, "no such volume")
	}
	volume.m.RLock()
	defer volume.m.RUnlock()
	return &ps.StateResponse{
		Size: volume.Content,
	}, nil
}

func (s *StorageServer) Migrate(ctx context.Context, request *ps.MigrateRequest) (*ps.MigrateResponse, error) {
	data, err := s.getObject(request.VolumeId, request.Offset)
	if err != nil {
		return nil, err
	}
	response, err := s.putObject(string(data), "", false)
	if err != nil {
		return nil, err
	}
	return &ps.MigrateResponse{
		VolumeId: response.VolumeId,
		Offset:   response.Offset,
	}, nil
}

func (s *StorageServer) Rotate(ctx context.Context, request *ps.RotateRequest) (*ps.RotateResponse, error) {
	s.m.Lock()
	defer s.m.Unlock()
	s.addVolume()
	return &ps.RotateResponse{}, nil
}

func (s *StorageServer) DeleteVolume(ctx context.Context, request *ps.DeleteVolumeRequest) (*ps.DeleteVolumeResponse, error) {
	volumeID := request.VolumeId
	s.m.Lock()
	defer s.m.Unlock()
	if volumeID == s.CurrentVolume {
		return nil, status.Error(codes.FailedPrecondition, "cannot delete current volume")
	}
	volume, ok := s.Volumes[volumeID]
	if !ok {
		return nil, status.Error(codes.NotFound, "no such volume")
	}
	volume.m.Lock()
	defer volume.m.Unlock()
	if volume.BlockSize == -1 {
		err := os.Remove(path.Join(s.Root, fmt.Sprintf("%d.dat", volumeID)))
		if err != nil {
			logrus.WithError(err).Warnf("Delete volume %v failed", volumeID)
		}
	} else {
		for _, block := range volume.Blocks {
			err := os.Remove(block.Path)
			if err != nil {
				logrus.WithError(err).Warnf("Delete block %s failed", block.Path)
			}
		}
	}
	delete(s.Volumes, volumeID)
	return &ps.DeleteVolumeResponse{}, nil
}

func (s *StorageServer) Get(ctx context.Context, request *ps.GetRequest) (*ps.GetResponse, error) {
	data, err := s.getObject(request.VolumeId, request.Offset)
	return &ps.GetResponse{
		Body: string(data),
	}, err
}

func (s *StorageServer) getObject(id, offset int64) ([]byte, error) {
	s.m.RLock()
	volume, ok := s.Volumes[id]
	if !ok {
		s.m.RUnlock()
		return nil, status.Error(codes.NotFound, "no such volume")
	}
	volume.m.RLock()
	s.m.RUnlock()
	var data []byte
	if volume.BlockSize != -1 {
		volume.m.RUnlock()
		bytes, err := volume.readFromBlocks(offset, 8, s.config)
		if err != nil {
			return nil, err
		}
		data, err = volume.readFromBlocks(offset+8, int64(binary.BigEndian.Uint64(bytes)), s.config)
		if err != nil {
			return nil, err
		}
	} else {
		defer volume.m.RUnlock()
		name := path.Join(s.Root, fmt.Sprintf("%d.dat", id))
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
		data = make([]byte, int64(binary.BigEndian.Uint64(bytes)))
		_, err = file.ReadAt(data, offset+8)
		if err != nil {
			logrus.WithError(err).Errorf("Read file %v failed", name)
			return nil, status.Error(codes.Internal, "read data failed")
		}
	}
	return data, nil
}

func (s *StorageServer) Put(ctx context.Context, request *ps.PutRequest) (*ps.PutResponse, error) {
	return s.putObject(request.Body, request.Tag, true)
}

func (s *StorageServer) putObject(data, rtag string, check bool) (*ps.PutResponse, error) {
	if check && fmt.Sprintf("%x", sha256.Sum256([]byte(data))) != rtag {
		return nil, status.Error(codes.Unauthenticated, "data operation not authenticated")
	}
	size := int64(len(data))
	s.m.Lock()
	id := s.CurrentVolume
	volume := s.Volumes[id]
	volume.m.Lock()
	offset := volume.Size
	volume.Size += 8 + size
	volume.Content += size
	if volume.Size >= s.config.VolumeMaxSize {
		s.addVolume()
	}
	defer volume.m.Unlock()
	s.m.Unlock()
	name := path.Join(s.Root, fmt.Sprintf("%d.dat", id))
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
		VolumeId: id,
		Offset:   offset,
	}
	return response, nil
}

func (s *StorageServer) addVolume() {
	logrus.Infof("Volume index increase from %v to %v", s.CurrentVolume, s.CurrentVolume+1)
	volume, ok := s.Volumes[s.CurrentVolume]
	if ok && volume.Size != 0 {
		go volume.encode(path.Join(s.Root, fmt.Sprintf("%d.dat", volume.ID)), s.chooseValidBSDs())
	}
	s.CurrentVolume++
	s.Volumes[s.CurrentVolume] = NewVolume(s.CurrentVolume, s.config)
}

func (s *StorageServer) chooseValidBSDs() []*BSD {
	nums := generateRandomNumber(0, s.config.BSDNum, s.config.DataShard+s.config.ParityShard)
	res := make([]*BSD, 0)
	for _, num := range nums {
		res = append(res, s.BSDs[int64(num)])
	}
	return res
}
