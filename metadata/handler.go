package metadata

import (
	"context"
	"encoding/json"
	"io/ioutil"
	"os"
	pm "oss/proto/metadata"
	ps "oss/proto/storage"
	"path"
	"time"

	"github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var (
	LayerKeyThreshold  = 1000
	LayerSizeThreshold = int64(1 << 30)
	DumpFileName       = "metadata.json"
	DumpInterval       = 5 * time.Second
	HeartbeatTimeout   = 10 * time.Second
)

type MetadataServer struct {
	pm.MetadataForStorageServer `json:"-"`
	pm.MetadataForProxyServer   `json:"-"`

	Root           string `json:"-"`
	Address        string
	Bucket         map[string]*Bucket
	storageTimer   map[string]time.Time
	storageClients map[string]ps.StorageForMetadataClient
}

func NewMetadataServer(address string, root string) *MetadataServer {
	s := &MetadataServer{
		Root:           root,
		Address:        address,
		Bucket:         make(map[string]*Bucket),
		storageTimer:   make(map[string]time.Time),
		storageClients: make(map[string]ps.StorageForMetadataClient),
	}
	s.recover()
	go s.dumpLoop()
	go s.heartbeatLoop()
	return s
}

func (s *MetadataServer) recover() {
	file, err := os.Open(path.Join(s.Root, DumpFileName))
	if err != nil {
		logrus.WithError(err).Error("Open recover file failed")
		return
	}
	defer file.Close()
	bytes, err := ioutil.ReadAll(file)
	if err != nil {
		logrus.WithError(err).Error("Read recover file failed")
		return
	}
	err = json.Unmarshal(bytes, &s)
	if err != nil {
		logrus.WithError(err).Error("Unmarshal JSON failed")
	}
}

func (s *MetadataServer) heartbeatLoop() {
	ticker := time.NewTicker(HeartbeatTimeout)
	for {
		for address, t := range s.storageTimer {
			if t.Add(HeartbeatTimeout).Before(time.Now()) {
				logrus.WithField("address", address).Warn("Close expired storage connenction")
				delete(s.storageTimer, address)
				delete(s.storageClients, address)
			}
		}
		<-ticker.C
	}
}

func (s *MetadataServer) dumpLoop() {
	ticker := time.NewTicker(DumpInterval)
	for {
		<-ticker.C
		bytes, err := json.Marshal(s)
		if err != nil {
			logrus.WithError(err).Error("Marshal JSON failed")
			continue
		}
		file, err := os.OpenFile(path.Join(s.Root, DumpFileName), os.O_CREATE|os.O_WRONLY, 0766)
		if err != nil {
			logrus.WithError(err).Error("Open dump file falied")
			continue
		}
		_, err = file.Write(bytes)
		if err != nil {
			logrus.WithError(err).Error("Write dump file failed")
		}
		file.Close()
	}
}

func (s *MetadataServer) searchEntry(bucket *Bucket, key string) (*Entry, error) {
	entry, ok := bucket.MemoMap[key]
	if ok {
		return entry, nil
	}
	for i := len(bucket.SSTable) - 1; i >= 0; i-- {
		layer := bucket.SSTable[i]
		file, err := os.Open(layer.Name)
		if err != nil {
			logrus.WithError(err).Errorf("Open file %v failed", layer.Name)
			return nil, status.Error(codes.Internal, "open index failed")
		}
		bytes, err := ioutil.ReadAll(file)
		if err != nil {
			logrus.WithError(err).Errorf("Read file %v failed", layer.Name)
			return nil, status.Error(codes.Internal, "read index failed")
		}
		entryList := make([]*Entry, 0)
		err = json.Unmarshal(bytes, &entryList)
		if err != nil {
			logrus.WithError(err).Errorf("Unmarshal JSON from file %v failed", layer.Name)
			return nil, status.Error(codes.DataLoss, "index data corrupted")
		}
		l, h := 0, len(entryList)-1
		for l <= h {
			m := l + (h-l)/2
			if entryList[m].Key == key {
				return entryList[m], nil
			} else if entryList[m].Key > key {
				h = m - 1
			} else {
				l = m + 1
			}
		}
	}
	return nil, nil
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
	s.storageTimer[address] = time.Now()
	return &pm.HeartbeatResponse{}, nil
}

func (s *MetadataServer) CreateBucket(ctx context.Context, request *pm.CreateBucketRequest) (*pm.CreateBucketResponse, error) {
	bucketName := request.Bucket
	_, ok := s.Bucket[bucketName]
	if ok {
		return nil, status.Error(codes.AlreadyExists, "bucket already exist")
	}
	logrus.WithField("bucket", request.Bucket).Debug("Creat new bucket")
	bucket := &Bucket{
		Name:     bucketName,
		TagMap:   make(map[string]string),
		MemoMap:  make(map[string]*Entry),
		SSTable:  make([]*Layer, 0),
		MemoSize: 0,
	}
	s.Bucket[bucketName] = bucket
	return &pm.CreateBucketResponse{}, nil
}

func (s *MetadataServer) CheckMeta(ctx context.Context, request *pm.CheckMetaRequest) (*pm.CheckMetaResponse, error) {
	bucket, ok := s.Bucket[request.Bucket]
	if !ok {
		return nil, status.Error(codes.NotFound, "bucket not found")
	}
	key, ok := bucket.TagMap[request.Tag]
	if ok {
		e, err := s.searchEntry(bucket, key)
		if err != nil {
			return nil, err
		}
		if e != nil {
			entry := &Entry{
				Key:     request.Key,
				Tag:     request.Tag,
				Address: e.Address,
				Volume:  e.Volume,
				Offset:  e.Offset,
				Size:    e.Size,
			}
			bucket.MemoSize += e.Size
			bucket.MemoMap[request.Key] = entry
			return &pm.CheckMetaResponse{
				Existed: true,
				Address: "",
			}, nil
		}
	}
	for address := range s.storageClients {
		return &pm.CheckMetaResponse{
			Existed: false,
			Address: address,
		}, nil
	}
	return nil, status.Error(codes.Unavailable, "")
}

func (s *MetadataServer) PutMeta(ctx context.Context, request *pm.PutMetaRequest) (*pm.PutMetaResponse, error) {
	bucket, ok := s.Bucket[request.Bucket]
	if !ok {
		return nil, status.Error(codes.NotFound, "bucket not exist")
	}
	entry := &Entry{
		Key:     request.Key,
		Tag:     request.Tag,
		Address: request.Address,
		Volume:  request.VolumeId,
		Offset:  request.Offset,
		Size:    request.Size,
	}
	if len(bucket.MemoMap) > LayerKeyThreshold || bucket.MemoSize > LayerSizeThreshold {
		err := bucket.createNewLayer()
		if err != nil {
			return nil, err
		}
	}
	bucket.TagMap[request.Tag] = request.Key
	bucket.MemoMap[request.Key] = entry
	bucket.MemoSize += request.Size
	return &pm.PutMetaResponse{}, nil
}
func (s *MetadataServer) GetMeta(ctx context.Context, request *pm.GetMetaRequest) (*pm.GetMetaResponse, error) {
	bucket, ok := s.Bucket[request.Bucket]
	if !ok {
		return nil, status.Error(codes.NotFound, "bucket not exist")
	}
	entry, err := s.searchEntry(bucket, request.Key)
	if err != nil {
		return nil, err
	}
	if entry == nil {
		return nil, status.Error(codes.NotFound, "object metadata not found")
	}
	return &pm.GetMetaResponse{
		Address:  entry.Address,
		VolumeId: int64(entry.Volume),
		Offset:   int64(entry.Offset),
	}, nil
}
