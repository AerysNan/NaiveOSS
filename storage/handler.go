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

type Config struct {
	VolumeMaxSize    int64
	DumpFileName     string
	DataShard        int
	ParityShard      int
	BSDNum           int
	CheckBSDTimeout  time.Duration
	DumpTimeout      time.Duration
	HeartbeatTimeout time.Duration
}

type BSD struct {
	Address string
}

func NewBSD(address string) *BSD {
	return &BSD{
		Address: address,
	}
}

type Block struct {
	CheckSum string
	Path     string
}

func NewBlock(checkSum string, path string) *Block {
	return &Block{
		CheckSum: checkSum,
		Path:     path,
	}
}

type Volume struct {
	m         sync.RWMutex
	ID        int64
	Size      int64
	Content   int64
	BlockSize int64
	Blocks    []*Block
}

func NewVolume(id int64, config *Config) *Volume {
	return &Volume{
		m:         sync.RWMutex{},
		ID:        id,
		Size:      0,
		Content:   0,
		BlockSize: -1,
		Blocks:    make([]*Block, 0, config.DataShard+config.ParityShard),
	}
}

func (v *Volume) readFromBlocks(offset, length int64, enc *reedsolomon.Encoder, config *Config) ([]byte, error) {
	sequence := offset / v.BlockSize
	offset = offset % v.BlockSize
	var data []byte
	for {
		block := v.Blocks[sequence]
		v.m.RLock()
		f, err := ioutil.ReadFile(block.Path)
		v.m.RUnlock()
		if err != nil || fmt.Sprintf("%x", sha256.Sum256(f)) != block.CheckSum {
			err = v.reconstructVolume(enc, config)
			if err != nil {
				return nil, err
			}
		}
		v.m.RLock()
		file, err := os.Open(block.Path)
		if err != nil {
			v.m.RUnlock()
			logrus.WithError(err).Errorf("Open file %v failed", block.Path)
			return nil, status.Error(codes.Internal, "open data failed")
		}
		bytes := make([]byte, length)
		n, err := file.ReadAt(bytes, offset)
		file.Close()
		v.m.RUnlock()
		if err != nil && n == 0 {
			logrus.WithError(err).Errorf("Read file %v failed", block.Path)
			return nil, status.Error(codes.Internal, "read data failed")
		}
		length -= int64(n)
		data = append(data, bytes[:n]...)
		if length > 0 {
			sequence++
			offset = 0
			continue
		} else {
			break
		}
	}
	return data, nil
}

func (v *Volume) encodeVolume(name string, BSDs []*BSD, enc *reedsolomon.Encoder) {
	v.m.RLock()
	f, err := ioutil.ReadFile(name)
	v.m.RUnlock()
	if err != nil {
		logrus.WithError(err).Errorf("Open file %v failed while encoding", name)
		return
	}
	blocks, err := (*enc).Split(f)
	if err != nil {
		logrus.WithError(err).Errorf("Split file %v failed while encoding", name)
		return
	}
	err = (*enc).Encode(blocks)
	if err != nil {
		logrus.WithError(err).Errorf("Encode file %v failed while encoding", name)
		return
	}
	for i, block := range blocks {
		path := path.Join(BSDs[i].Address, fmt.Sprintf("%d_%d.dat", v.ID, i))
		tag := fmt.Sprintf("%x", sha256.Sum256(blocks[i]))
		v.Blocks = append(v.Blocks, NewBlock(tag, path))
		err := ioutil.WriteFile(path, block, os.ModePerm)
		if err != nil {
			logrus.WithError(err).Errorf("Write file %s failed", path)
		}
	}
	v.m.Lock()
	v.BlockSize = int64(len(blocks[0]))
	_ = os.Remove(name)
	v.m.Unlock()
}

func (v *Volume) reconstructVolume(enc *reedsolomon.Encoder, config *Config) error {
	v.m.RLock()
	needRepair := make([]int, 0)
	data := make([][]byte, config.DataShard+config.ParityShard)
	for i, block := range v.Blocks {
		f, err := ioutil.ReadFile(block.Path)
		if err != nil || fmt.Sprintf("%x", sha256.Sum256(f)) != block.CheckSum {
			logrus.WithField("VolumeID", v.ID).Warnf("%s need reconstructing", block.Path)
			data[i] = nil
			needRepair = append(needRepair, i)
		} else {
			data[i] = f
		}
	}
	v.m.RUnlock()
	if len(needRepair) == 0 {
		return nil
	}
	logrus.WithField("VolumeID", v.ID).Warn("Start reconstructing data")
	err := (*enc).Reconstruct(data)
	if err != nil {
		logrus.WithField("VolumeID", v.ID).Error("Data corrupted")
		return status.Error(codes.Internal, "Data Corrupted")
	}
	ok, err := (*enc).Verify(data)
	if err != nil || !ok {
		logrus.WithField("VolumeID", v.ID).Error("Data corrupted")
		return status.Error(codes.Internal, "Data Corrupted")
	} else {
		logrus.WithField("VolumeID", v.ID).Info("Reconstructing data succeed")
		v.m.Lock()
		for _, n := range needRepair {
			err := ioutil.WriteFile(v.Blocks[n].Path, data[n], os.ModePerm)
			if err != nil {
				logrus.WithError(err).Errorf("Write file %s failed after reconstructing", v.Blocks[n].Path)
			}
		}
		v.m.Unlock()
	}
	return nil
}

type StorageServer struct {
	ps.StorageForProxyServer
	ps.StorageForMetadataServer
	metadataClient pm.MetadataForStorageClient

	m             sync.RWMutex
	enc           reedsolomon.Encoder
	config        *Config
	Address       string `json:"-"`
	Root          string `json:"-"`
	BSDs          map[int64]*BSD
	Volumes       map[int64]*Volume
	CurrentVolume int64
}

func NewStorageServer(address string, Root string, metadataClient pm.MetadataForStorageClient, config *Config) *StorageServer {
	storageServer := &StorageServer{
		metadataClient: metadataClient,
		m:              sync.RWMutex{},
		config:         config,
		Address:        address,
		Root:           Root,
		BSDs:           make(map[int64]*BSD),
		Volumes:        make(map[int64]*Volume),
		CurrentVolume:  0,
	}
	storageServer.recover()
	storageServer.encoderInit()
	storageServer.bsdInit()
	storageServer.addVolume()
	go storageServer.dumpLoop()
	go storageServer.heartbeatLoop()
	go storageServer.checkBSDLoop()
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

func (s *StorageServer) encoderInit() {
	var err error
	s.enc, err = reedsolomon.New(s.config.DataShard, s.config.ParityShard)
	if err != nil {
		logrus.WithError(err).Error("Encoder init failed")
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

func (s *StorageServer) checkBSDLoop() {
	ticker := time.NewTicker(s.config.CheckBSDTimeout)
	for {
		func() {
			s.m.RLock()
			volumes := s.Volumes
			currentId := s.CurrentVolume
			s.m.RUnlock()
			for i := int64(1); i < currentId; i++ {
				volumes[i].m.RLock()
				blocks := volumes[i].Blocks
				volumes[i].m.RUnlock()
				for _, block := range blocks {
					f, err := ioutil.ReadFile(block.Path)
					if err != nil || fmt.Sprintf("%x", sha256.Sum256(f)) != block.CheckSum {
						volumes[i].reconstructVolume(&s.enc, s.config)
						break
					}
				}
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
	data, err := s.getObject(request.VolumeId, request.Offset)
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
	s.m.RUnlock()
	if !ok {
		return nil, status.Error(codes.NotFound, "no such volume")
	}
	var data []byte
	if volume.BlockSize != -1 {
		bytes, err := volume.readFromBlocks(offset, 8, &s.enc, s.config)
		if err != nil {
			return nil, err
		}
		data, err = volume.readFromBlocks(offset+8, int64(binary.BigEndian.Uint64(bytes)), &s.enc, s.config)
		if err != nil {
			return nil, err
		}
	} else {
		volume.m.RLock()
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
	s.m.RLock()
	name := path.Join(s.Root, fmt.Sprintf("%d.dat", s.CurrentVolume))
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
	logrus.Infof("Volume index increase from %v to %v", s.CurrentVolume, s.CurrentVolume+1)
	volume, ok := s.Volumes[s.CurrentVolume]
	if ok && volume.Size != 0 {
		go volume.encodeVolume(path.Join(s.Root, fmt.Sprintf("%d.dat", volume.ID)), s.chooseValidBSDs(), &s.enc)
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
