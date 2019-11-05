package metadata

import (
	"context"
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
	CompactTimeout     = time.Minute
	TrashRateThreshold = 0.4
)

type MetadataServer struct {
	pm.MetadataForStorageServer `json:"-"`
	pm.MetadataForProxyServer   `json:"-"`

	m              sync.RWMutex
	ref            map[int64]int64
	Root           string `json:"-"`
	Address        string
	TagMap         map[string]*EntryMeta
	Bucket         map[string]*Bucket
	storageTimer   map[string]time.Time
	storageClients map[string]ps.StorageForMetadataClient
}

func NewMetadataServer(address string, root string) *MetadataServer {
	s := &MetadataServer{
		m:              sync.RWMutex{},
		ref:            make(map[int64]int64),
		Root:           root,
		Address:        address,
		TagMap:         make(map[string]*EntryMeta),
		Bucket:         make(map[string]*Bucket),
		storageTimer:   make(map[string]time.Time),
		storageClients: make(map[string]ps.StorageForMetadataClient),
	}
	s.recover()
	go s.dumpLoop()
	go s.compactLoop()
	go s.heartbeatLoop()
	return s
}

func (s *MetadataServer) recover() {
	filePath := path.Join(s.Root, DumpFileName)
	_, err := os.Stat(filePath)
	if os.IsNotExist(err) {
		return
	}
	file, err := os.Open(filePath)
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
	for _, bucket := range s.Bucket {
		bucket.m = sync.RWMutex{}
	}
}

func (s *MetadataServer) compactLoop() {
	ticker := time.NewTicker(CompactTimeout)
	for {
		wg := sync.WaitGroup{}
		s.m.RLock()
		wg.Add(len(s.Bucket))
		for _, bucket := range s.Bucket {
			go func(b *Bucket) {
				defer wg.Done()
				if len(b.SSTable) <= 1 {
					return
				}
				entries, err := mergeLayers(b.SSTable)
				if err != nil {
					logrus.WithError(err).Error("Merge layers failed")
					return
				}
				v2Size := make(map[string]int64)
				for _, entry := range entries {
					key := fmt.Sprintf("%v-%v", entry.Address, entry.Volume)
					v2Size[key] += entry.Size
				}
				layers := [][]*Entry{entries}
				for key, size := range v2Size {
					var address string
					var volume int64
					fmt.Sscanf(key, "%v-%v", &address, &volume)
					state, err := s.storageClients[address].State(context.Background(), &ps.StateRequest{
						VolumeId: volume,
					})
					if err != nil {
						logrus.WithError(err).Error("Get volume state failed")
						return
					}
					trashRate := 1.0 - float64(size)/float64(state.Size)
					if trashRate > TrashRateThreshold {
						newEntries, err := s.compress(address, volume, entries)
						if err == nil {
							logrus.Debug("Compress volume succeeded")
							layers = append(layers, newEntries)
						} else {
							logrus.WithError(err).Error("Compress volume failed")
							return
						}
					}
				}
				set := make(map[int64]struct{})
				entries = mergeEntryMatrix(layers)
				for _, entry := range entries {
					set[entry.Volume] = struct{}{}
				}

				volumeIDs := make([]int64, 0)
				for volumeID := range set {
					volumeIDs = append(volumeIDs, volumeID)
				}
				b.SSTable = []*Layer{
					&Layer{
						Name:    "haha",
						Volumes: volumeIDs,
					},
				}
			}(bucket)
		}
		s.m.RUnlock()
		wg.Wait()
		<-ticker.C
	}
}

func (s *MetadataServer) compress(address string, volumeID int64, entries []*Entry) ([]*Entry, error) {
	logrus.WithFields(logrus.Fields{
		"address":  address,
		"volumeID": volumeID,
	}).Debug("Begin to compress volume")
	resultEntries := make([]*Entry, 0)
	size := int64(0)
	for _, entry := range entries {
		if entry.Volume == volumeID && !entry.Delete {
			resultEntries = append(resultEntries, entry)
			size += entry.Size
		}
	}
	if size == 0 {
		logrus.WithFields(logrus.Fields{
			"address":  address,
			"volumeID": volumeID,
		}).Debug("Remove empty volume")
	}
	// TODO
	return nil, nil
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
		func() {
			bytes, err := json.Marshal(s)
			if err != nil {
				logrus.WithError(err).Error("Marshal JSON failed")
				return
			}
			file, err := os.OpenFile(path.Join(s.Root, DumpFileName), os.O_CREATE|os.O_WRONLY, 0766)
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

func (s *MetadataServer) searchEntry(bucket *Bucket, key string) (*Entry, error) {
	entry, ok := bucket.MemoMap[key]
	if ok {
		return entry, nil
	}
	for i := len(bucket.SSTable) - 1; i >= 0; i-- {
		layer := bucket.SSTable[i]
		entryList, err := readLayer(layer.Name)
		if err != nil {
			logrus.Errorf("Get layer %v content failed", layer.Name)
			return nil, err
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
	s.m.Lock()
	defer s.m.Unlock()
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
	s.m.Lock()
	defer s.m.Unlock()
	_, ok := s.Bucket[bucketName]
	if ok {
		return nil, status.Error(codes.AlreadyExists, "bucket already exist")
	}
	logrus.WithField("bucket", bucketName).Debug("Create new bucket")
	bucket := &Bucket{
		m:          sync.RWMutex{},
		Name:       bucketName,
		MemoMap:    make(map[string]*Entry),
		SSTable:    make([]*Layer, 0),
		MemoSize:   0,
		CreateTime: time.Now().Unix(),
	}
	s.Bucket[bucketName] = bucket
	return &pm.CreateBucketResponse{}, nil
}

func (s *MetadataServer) DeleteBucket(ctx context.Context, request *pm.DeleteBucketRequest) (*pm.DeleteBucketResponse, error) {
	bucketName := request.Bucket
	s.m.Lock()
	defer s.m.Unlock()
	_, ok := s.Bucket[bucketName]
	if !ok {
		return nil, status.Error(codes.NotFound, "bucket not found")
	}
	logrus.WithField("bucket", bucketName).Debug("Delete bucket")
	delete(s.Bucket, bucketName)
	return &pm.DeleteBucketResponse{}, nil
}

func (s *MetadataServer) CheckMeta(ctx context.Context, request *pm.CheckMetaRequest) (*pm.CheckMetaResponse, error) {
	s.m.RLock()
	defer s.m.RUnlock()
	bucket, ok := s.Bucket[request.Bucket]
	if !ok {
		return nil, status.Error(codes.NotFound, "bucket not found")
	}
	meta, ok := s.TagMap[request.Tag]
	if ok {
		entry := &Entry{
			Key:        request.Key,
			Tag:        request.Tag,
			Address:    meta.Address,
			Volume:     meta.Volume,
			Offset:     meta.Offset,
			Size:       meta.Size,
			CreateTime: time.Now().Unix(),
			Delete:     false,
		}
		bucket.MemoMap[request.Key] = entry
		return &pm.CheckMetaResponse{
			Existed: true,
			Address: "",
		}, nil
	}
	for address := range s.storageClients {
		return &pm.CheckMetaResponse{
			Existed: false,
			Address: address,
		}, nil
	}
	return nil, status.Error(codes.Unavailable, "no storage device available")
}

func (s *MetadataServer) PutMeta(ctx context.Context, request *pm.PutMetaRequest) (*pm.PutMetaResponse, error) {
	s.m.RLock()
	bucket, ok := s.Bucket[request.Bucket]
	if !ok {
		s.m.RUnlock()
		return nil, status.Error(codes.NotFound, "bucket not exist")
	}
	bucket.m.Lock()
	defer bucket.m.Unlock()
	s.m.RUnlock()
	entry := &Entry{
		Key:        request.Key,
		Tag:        request.Tag,
		Address:    request.Address,
		Volume:     request.VolumeId,
		Offset:     request.Offset,
		Size:       request.Size,
		CreateTime: time.Now().Unix(),
	}
	if len(bucket.MemoMap) > LayerKeyThreshold || bucket.MemoSize > LayerSizeThreshold {
		volumes, err := bucket.rotate()
		if err != nil {
			return nil, err
		}
		for _, v := range volumes {
			s.ref[v]++
		}
	}
	bucket.MemoMap[request.Key] = entry
	bucket.MemoSize += request.Size
	s.TagMap[request.Tag] = &EntryMeta{
		Address: request.Address,
		Volume:  request.VolumeId,
		Offset:  request.Offset,
		Size:    request.Size,
	}
	return &pm.PutMetaResponse{}, nil
}

func (s *MetadataServer) GetMeta(ctx context.Context, request *pm.GetMetaRequest) (*pm.GetMetaResponse, error) {
	s.m.RLock()
	bucket, ok := s.Bucket[request.Bucket]
	if !ok {
		s.m.RUnlock()
		return nil, status.Error(codes.NotFound, "bucket not exist")
	}
	bucket.m.RLock()
	defer bucket.m.RUnlock()
	s.m.RUnlock()
	entry, err := s.searchEntry(bucket, request.Key)
	if err != nil {
		return nil, err
	}
	if entry == nil || entry.Delete {
		return nil, status.Error(codes.NotFound, "object metadata not found")
	}
	return &pm.GetMetaResponse{
		Address:    entry.Address,
		VolumeId:   int64(entry.Volume),
		Offset:     int64(entry.Offset),
		Size:       int64(entry.Size),
		CreateTime: int64(entry.CreateTime),
	}, nil
}

func (s *MetadataServer) DeleteMeta(ctx context.Context, request *pm.DeleteMetaRequest) (*pm.DeleteMetaResponse, error) {
	s.m.RLock()
	bucket, ok := s.Bucket[request.Bucket]
	if !ok {
		s.m.RUnlock()
		return nil, status.Error(codes.NotFound, "bucket not exist")
	}
	bucket.m.Lock()
	defer bucket.m.Unlock()
	s.m.RUnlock()
	entry, err := s.searchEntry(bucket, request.Key)
	if err != nil {
		return nil, err
	}
	if entry == nil || entry.Delete {
		return nil, status.Error(codes.NotFound, "object metadata not found")
	}
	_, ok = bucket.MemoMap[request.Key]
	if ok {
		bucket.MemoMap[request.Key].Delete = true
	} else {
		bucket.MemoMap[request.Key] = entry
		entry.Delete = true
	}
	return &pm.DeleteMetaResponse{}, nil
}
