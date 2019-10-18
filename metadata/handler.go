package metadata

import (
	"context"
	"encoding/json"
	"io/ioutil"
	"os"
	"oss/osserror"
	pm "oss/proto/metadata"
	ps "oss/proto/storage"

	"github.com/sirupsen/logrus"
	"google.golang.org/grpc"
)

var (
	LayerKeyThreshold  = 1000
	LayerSizeThreshold = 1 << 30

	DefaultBucketName = "default"
	DeletedValue      = "ObjectDeleted"
	DeletedTag        = "750b7395a7c2506ef974069bb817d59e"
)

type MetadataServer struct {
	pm.MetadataForStorageServer
	pm.MetadataForProxyServer

	bucket         map[string]*Bucket
	address        string
	storageClients map[string]ps.StorageForMetadataClient
}

func NewMetadataServer(address string) *MetadataServer {
	return &MetadataServer{
		address:        address,
		bucket:         make(map[string]*Bucket),
		storageClients: make(map[string]ps.StorageForMetadataClient),
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
			return nil, osserror.ErrServerInternal
		}
		bytes, err := ioutil.ReadAll(file)
		if err != nil {
			return nil, osserror.ErrServerInternal
		}
		entryList := make([]*Entry, 0)
		err = json.Unmarshal(bytes, &entryList)
		if err != nil {
			return nil, osserror.ErrCorruptedFile
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

func (s *MetadataServer) Register(ctx context.Context, request *pm.RegisterRequest) (*pm.RegisterResponse, error) {
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
	return &pm.RegisterResponse{}, nil
}

func (s *MetadataServer) CreateBucket(ctx context.Context, request *pm.CreateBucketRequest) (*pm.CreateBucketResponse, error) {
	bucketName := request.Bucket
	_, ok := s.bucket[bucketName]
	if ok {
		return nil, osserror.ErrBucketAlreadyExist
	}
	logrus.WithField("bucket", request.Bucket).Debug("Creat new bucket")
	bucket := &Bucket{
		Name:     bucketName,
		TagMap:   make(map[string]string),
		MemoMap:  make(map[string]*Entry),
		SSTable:  make([]*Layer, 0),
		MemoSize: 0,
	}
	s.bucket[bucketName] = bucket
	return &pm.CreateBucketResponse{}, nil
}

func (s *MetadataServer) CheckMeta(ctx context.Context, request *pm.CheckMetaRequest) (*pm.CheckMetaResponse, error) {
	bucket, ok := s.bucket[request.Bucket]
	if !ok {
		return nil, osserror.ErrBucketNotExist
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
			}
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
	return nil, osserror.ErrNoStorageAvailable
}

func (s *MetadataServer) PutMeta(ctx context.Context, request *pm.PutMetaRequest) (*pm.PutMetaResponse, error) {
	bucket, ok := s.bucket[request.Bucket]
	if !ok {
		return nil, osserror.ErrBucketNotExist
	}
	entry := &Entry{
		Key:     request.Key,
		Tag:     request.Tag,
		Address: request.Address,
		Volume:  int(request.VolumeId),
		Offset:  int(request.Offset),
		Size:    int(request.Size),
	}
	if len(bucket.MemoMap) > LayerKeyThreshold || bucket.MemoSize > LayerSizeThreshold {
		err := bucket.createNewLayer()
		if err != nil {
			return nil, err
		}
	}
	bucket.TagMap[request.Tag] = request.Key
	bucket.MemoMap[request.Key] = entry
	bucket.MemoSize += int(request.Size)
	return &pm.PutMetaResponse{}, nil
}
func (s *MetadataServer) GetMeta(ctx context.Context, request *pm.GetMetaRequest) (*pm.GetMetaResponse, error) {
	bucket, ok := s.bucket[request.Bucket]
	if !ok {
		return nil, osserror.ErrBucketNotExist
	}
	entry, err := s.searchEntry(bucket, request.Key)
	if err != nil {
		return nil, err
	}
	if entry == nil {
		return nil, osserror.ErrObjectMetadataNotFound
	}
	return &pm.GetMetaResponse{
		Address:  entry.Address,
		VolumeId: int64(entry.Volume),
		Offset:   int64(entry.Offset),
	}, nil
}
