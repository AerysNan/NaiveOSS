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
	"oss/raft"

	"github.com/klauspost/reedsolomon"
	"github.com/natefinch/atomic"
	"github.com/sirupsen/logrus"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var enc reedsolomon.Encoder

// Config for storage server
type Config struct {
	VolumeMaxSize    int64
	DumpFileName     string
	DataShard        int
	ParityShard      int
	BSDNum           int
	DumpTimeout      time.Duration
	HeartbeatTimeout time.Duration
	ValidBlobTimeout time.Duration
}

// Server represents storage server for storing object data
type Server struct {
	ps.StorageForProxyServer
	ps.StorageForMetadataServer
	metadataClient pm.MetadataForStorageClient

	m             sync.RWMutex
	rf            *raft.Raft
	config        *Config
	Address       string `json:"-"`
	Root          string `json:"-"`
	BSDs          map[int64]*BSD
	Blobs         map[string]*Blob
	Volumes       map[int64]*Volume
	CurrentVolume int64
}

func NewStorageServer(address string, root string, metadataClient pm.MetadataForStorageClient, config *Config) *Server {
	encoderInit(config)
	storageServer := &Server{
		metadataClient: metadataClient,
		m:              sync.RWMutex{},
		config:         config,
		Address:        address,
		Root:           root,
		Blobs:          make(map[string]*Blob),
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

func (s *Server) recover() {
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

func (s *Server) bsdInit() {
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

func (s *Server) heartbeatLoop() {
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

func (s *Server) dumpLoop() {
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

func (s *Server) CheckBlob(ctx context.Context, request *ps.CheckBlobRequest) (*ps.CheckBlobResponse, error) {
	s.m.RLock()
	blobs := s.Blobs
	s.m.RUnlock()
	result := make([]string, 0)
	for _, blob := range blobs {
		if blob.Time.Add(s.config.ValidBlobTimeout).Before(time.Now()) {
			name := path.Join(s.Root, fmt.Sprintf("%s.tmp", blob.Tag))
			result = append(result, blob.Id)
			s.m.Lock()
			delete(s.Blobs, blob.Tag)
			_ = os.Remove(name)
			logrus.WithField("name", name).Warn("Blob expired")
			s.m.Unlock()
		}
	}
	return &ps.CheckBlobResponse{
		Id: result,
	}, nil
}

func (s *Server) State(ctx context.Context, request *ps.StateRequest) (*ps.StateResponse, error) {
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

func (s *Server) Migrate(ctx context.Context, request *ps.MigrateRequest) (*ps.MigrateResponse, error) {
	data, err := s.getObject(request.VolumeId, request.Offset, 0)
	if err != nil {
		return nil, err
	}
	response, err := s.putObject(data)
	if err != nil {
		return nil, err
	}
	return &ps.MigrateResponse{
		VolumeId: response.VolumeId,
		Offset:   response.Offset,
	}, nil
}

// Rotate converts a writable volume to read only and create a new volume
func (s *Server) Rotate(ctx context.Context, request *ps.RotateRequest) (*ps.RotateResponse, error) {
	s.m.Lock()
	defer s.m.Unlock()
	s.addVolume()
	return &ps.RotateResponse{}, nil
}

// DeleteVolume handles delete volume request
func (s *Server) DeleteVolume(ctx context.Context, request *ps.DeleteVolumeRequest) (*ps.DeleteVolumeResponse, error) {
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

func (s *Server) Get(ctx context.Context, request *ps.GetRequest) (*ps.GetResponse, error) {
	data, err := s.getObject(request.VolumeId, request.Offset, request.Start)
	return &ps.GetResponse{
		Body: data,
	}, err
}

func (s *Server) getObject(id, offset, start int64) ([]byte, error) {
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
		size := int64(binary.BigEndian.Uint64(bytes))
		if start > size {
			return nil, status.Error(codes.InvalidArgument, "object offset overflow")
		}
		if start == size {
			return nil, status.Error(codes.Unknown, "empty data response")
		}
		data, err = volume.readFromBlocks(offset+8+start, size-start, s.config)
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
		size := int64(binary.BigEndian.Uint64(bytes))
		if start > size {
			return nil, status.Error(codes.InvalidArgument, "object offset overflow")
		}
		if start == size {
			return nil, status.Error(codes.Unknown, "empty data response")
		}
		data = make([]byte, size-start)
		_, err = file.ReadAt(data, offset+8+start)
		if err != nil {
			logrus.WithError(err).Errorf("Read file %v failed", name)
			return nil, status.Error(codes.Internal, "read data failed")
		}
	}
	return data, nil
}

func (s *Server) Create(ctx context.Context, request *ps.CreateRequest) (*ps.CreateResponse, error) {
	name := path.Join(s.Root, fmt.Sprintf("%s.tmp", request.Id))
	_, err := os.Create(name)
	if err != nil {
		logrus.WithError(err).Errorf("Create file %v failed", name)
		return nil, status.Error(codes.Internal, "create tmp file failed")
	}
	s.m.Lock()
	s.Blobs[request.Id] = NewBlob(request.Id, request.Tag)
	s.m.Unlock()
	return &ps.CreateResponse{}, nil
}

func (s *Server) Put(ctx context.Context, request *ps.PutRequest) (*ps.PutResponse, error) {
	name := path.Join(s.Root, fmt.Sprintf("%s.tmp", request.Id))
	blob, ok := s.Blobs[request.Id]
	if !ok {
		logrus.Errorf("file %v cleared", name)
		return nil, status.Error(codes.Internal, "tmp file cleared")
	}
	blob.m.Lock()
	defer blob.m.Unlock()
	file, err := os.OpenFile(name, os.O_WRONLY, 0766)
	defer func() {
		err := file.Close()
		if err != nil {
			logrus.WithError(err).Errorf("Close file %v failed", name)
		}
	}()
	if err != nil {
		logrus.WithError(err).Errorf("Open file %v failed", name)
		return nil, status.Error(codes.Internal, "open data failed")
	}
	_, err = file.WriteAt(request.Body, request.Offset)
	if err != nil {
		logrus.WithError(err).Errorf("Write file %v failed", name)
		return nil, status.Error(codes.Internal, "write data failed")
	}
	return &ps.PutResponse{}, nil
}

func (s *Server) Confirm(ctx context.Context, request *ps.ConfirmRequest) (*ps.ConfirmResponse, error) {
	name := path.Join(s.Root, fmt.Sprintf("%s.tmp", request.Id))
	blob, ok := s.Blobs[request.Id]
	if !ok {
		logrus.Errorf("file %v cleared", name)
		return nil, status.Error(codes.Internal, "tmp file cleared")
	}
	blob.m.Lock()
	defer blob.m.Unlock()
	file, err := os.Open(name)
	defer func() {
		err := file.Close()
		if err != nil {
			logrus.WithError(err).Errorf("Close file %v failed", name)
		}
	}()
	if err != nil {
		logrus.WithError(err).Errorf("Open file %v failed", name)
		return nil, status.Error(codes.Internal, "open data failed")
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		logrus.WithError(err).Errorf("Read file %v failed", name)
		return nil, status.Error(codes.Internal, "read data failed")
	}
	s.m.Lock()
	_ = os.Remove(name)
	delete(s.Blobs, request.Id)
	s.m.Unlock()
	if fmt.Sprintf("%x", sha256.Sum256(content)) != blob.Tag {
		logrus.Errorf("Upload file %v failed", name)
		return nil, status.Error(codes.Internal, "upload file failed")
	}
	return s.putObject(content)
}

func (s *Server) putObject(data []byte) (*ps.ConfirmResponse, error) {
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
	_, err = file.Write(data)
	if err != nil {
		logrus.WithError(err).Errorf("Write file %v failed", name)
		return nil, status.Error(codes.Internal, "write data failed")
	}
	response := &ps.ConfirmResponse{
		VolumeId: id,
		Offset:   offset,
	}
	return response, nil
}

func (s *Server) addVolume() {
	logrus.Infof("Volume index increase from %v to %v", s.CurrentVolume, s.CurrentVolume+1)
	volume, ok := s.Volumes[s.CurrentVolume]
	if ok && volume.Size != 0 {
		go volume.encode(path.Join(s.Root, fmt.Sprintf("%d.dat", volume.ID)), s.chooseValidBSDs())
	}
	s.CurrentVolume++
	s.Volumes[s.CurrentVolume] = NewVolume(s.CurrentVolume, s.config)
}

func (s *Server) chooseValidBSDs() []*BSD {
	nums := generateRandomNumber(0, s.config.BSDNum, s.config.DataShard+s.config.ParityShard)
	res := make([]*BSD, 0)
	for _, num := range nums {
		res = append(res, s.BSDs[int64(num)])
	}
	return res
}
