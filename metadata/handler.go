package metadata

import (
	"bytes"
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

	"github.com/natefinch/atomic"
	"github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type Config struct {
	DumpFileName       string
	LayerKeyThreshold  int
	TrashRateThreshold float64
	DumpTimeout        time.Duration
	HeartbeatTimeout   time.Duration
	CompactTimeout     time.Duration
	GCTimeout          time.Duration
}

type MetadataServer struct {
	pm.MetadataForStorageServer `json:"-"`
	pm.MetadataForProxyServer   `json:"-"`

	config         *Config
	m              sync.RWMutex
	ref            map[string]int64
	Root           string `json:"-"`
	Address        string `json:"-"`
	TagMap         map[string]*EntryMeta
	Bucket         map[string]*Bucket
	storageTimer   map[string]time.Time
	storageClients map[string]ps.StorageForMetadataClient
}

func NewMetadataServer(address string, root string, config *Config) *MetadataServer {
	s := &MetadataServer{
		m:              sync.RWMutex{},
		ref:            make(map[string]int64),
		config:         config,
		Root:           root,
		Address:        address,
		TagMap:         make(map[string]*EntryMeta),
		Bucket:         make(map[string]*Bucket),
		storageTimer:   make(map[string]time.Time),
		storageClients: make(map[string]ps.StorageForMetadataClient),
	}
	s.recover()
	go s.gcLoop()
	go s.dumpLoop()
	go s.compactLoop()
	go s.heartbeatLoop()
	return s
}

func (s *MetadataServer) recover() {
	filePath := path.Join(s.Root, s.config.DumpFileName)
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
		for _, layer := range bucket.SSTable {
			for _, volume := range layer.Volumes {
				logrus.WithField("action", "recover").Debugf("Reference increase on volume %v", volume)
				s.ref[volume]++
			}
		}
	}
}

func (s *MetadataServer) compactLoop() {
	ticker := time.NewTicker(s.config.CompactTimeout)
	for {
		wg := sync.WaitGroup{}
		s.m.Lock()
		wg.Add(len(s.Bucket))
		for _, bucket := range s.Bucket {
			go func(b *Bucket) {
				defer wg.Done()
				if len(b.SSTable) <= 1 {
					return
				}
				logrus.WithField("bucket", b.Name).Debug("Perform compact")
				entries, err := b.mergeLayers(b.SSTable)
				if err != nil {
					logrus.WithError(err).Error("Merge layers failed")
					return
				}
				v2Size := make(map[string]int64)
				for _, entry := range entries {
					key := fmt.Sprintf("%v-%v", entry.Volume, entry.Address)
					v2Size[key] += entry.Size
				}
				layers := [][]*Entry{entries}
				for key, size := range v2Size {
					var address string
					var volumeID int64
					fmt.Sscanf(key, "%v-%v", &volumeID, &address)
					client, ok := s.storageClients[address]
					if !ok {
						logrus.WithField("address", address).Error("Storage client not found")
						return
					}
					state, err := client.State(context.Background(), &ps.StateRequest{
						VolumeId: volumeID,
					})
					if err != nil {
						logrus.WithError(err).Error("Get volume state failed")
						return
					}
					trashRate := 1.0 - float64(size)/float64(state.Size)
					if trashRate > s.config.TrashRateThreshold {
						logrus.Debugf("Trashrate threshold exceeded, content %v, actual %v", size, state.Size)
						newEntries, err := s.compress(address, volumeID, entries)
						if err == nil {
							logrus.Debug("Compress volume succeeded")
							layers = append(layers, newEntries)
						} else {
							logrus.WithError(err).Error("Compress volume failed")
							return
						}
					}
				}
				entries = mergeEntryMatrix(layers)
				set := make(map[string]struct{})
				for _, entry := range entries {
					set[fmt.Sprintf("%v-%v", entry.Volume, entry.Address)] = struct{}{}
				}
				volumeIDs := make([]string, 0)
				for volumeID := range set {
					volumeIDs = append(volumeIDs, volumeID)
				}
				compactedLayer := &Layer{
					Name:    fmt.Sprintf("%v-%v-%v", b.Name, b.SSTable[0].Begin, b.SSTable[len(b.SSTable)-1].End),
					Volumes: volumeIDs,
					Begin:   b.SSTable[0].Begin,
					End:     b.SSTable[len(b.SSTable)-1].End,
				}
				for _, layer := range b.SSTable {
					b.deleteLayer(layer.Name)
					for _, volume := range layer.Volumes {
						logrus.WithField("action", "delete").Debugf("Reference decrease on volume %v", volume)
						s.ref[volume]--
					}
				}
				b.SSTable = []*Layer{}
				err = b.writeLayer(entries, volumeIDs, compactedLayer.Begin, compactedLayer.End)
				for _, volumeID := range volumeIDs {
					logrus.WithField("action", "compact").Debugf("Referenece increase on volume %v", volumeID)
					s.ref[volumeID]++
				}
				if err != nil {
					logrus.WithError(err).Error("Write layer failed")
					return
				}
			}(bucket)
		}
		s.m.Unlock()
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
		if entry.Address == address && entry.Volume == volumeID && !entry.Delete {
			resultEntries = append(resultEntries, entry)
			size += entry.Size
		}
	}
	if size == 0 {
		logrus.WithFields(logrus.Fields{
			"address":  address,
			"volumeID": volumeID,
		}).Debug("Remove empty volume")
		return []*Entry{}, nil
	}
	client, ok := s.storageClients[address]
	if !ok {
		return nil, status.Error(codes.NotFound, "no such storage client")
	}
	_, err := client.Rotate(context.Background(), &ps.RotateRequest{})
	if err != nil {
		return nil, status.Error(codes.NotFound, "create new volume failed")
	}
	for _, entry := range resultEntries {
		response, err := client.Migrate(context.Background(), &ps.MigrateRequest{
			VolumeId: entry.Volume,
			Offset:   entry.Offset,
		})
		if err != nil {
			return nil, err
		}
		entry.Volume = response.VolumeId
		entry.Offset = response.Offset
	}
	return resultEntries, nil
}

func (s *MetadataServer) heartbeatLoop() {
	ticker := time.NewTicker(s.config.HeartbeatTimeout)
	for {
		for address, t := range s.storageTimer {
			if t.Add(s.config.HeartbeatTimeout).Before(time.Now()) {
				logrus.WithField("address", address).Warn("Close expired storage connenction")
				delete(s.storageTimer, address)
				delete(s.storageClients, address)
			}
		}
		<-ticker.C
	}
}

func (s *MetadataServer) dumpLoop() {
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

func (s *MetadataServer) gcLoop() {
	ticker := time.NewTicker(s.config.GCTimeout)
	for {
		s.m.RLock()
		for volume, ref := range s.ref {
			if ref > 0 {
				continue
			}
			var address string
			var volumeID int64
			fmt.Sscanf(volume, "%v-%v", &volumeID, &address)
			found := func() bool {
				for _, bucket := range s.Bucket {
					for _, entry := range bucket.MemoMap {
						if entry.Address == address && entry.Volume == volumeID {
							return true
						}
					}
				}
				return false
			}()
			if found {
				continue
			}
			delete(s.ref, volume)
			logrus.WithField("volume", volume).Debug("Delete zero referenced volume")
			client, ok := s.storageClients[address]
			if !ok {
				logrus.WithField("address", address).Error("Storage client not found")
				continue
			}
			_, err := client.DeleteVolume(context.Background(), &ps.DeleteVolumeRequest{
				VolumeId: volumeID,
			})
			if err != nil {
				logrus.WithError(err).Error("GC useless volume failed")
			} else {
				logrus.Debug("GC useless volume succeeded")
			}
		}
		s.m.RUnlock()
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
		entryList, err := bucket.readLayer(layer.Name)
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
		root:       s.Root,
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
		bucket.m.Lock()
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
		if len(bucket.MemoMap) >= s.config.LayerKeyThreshold {
			volumes, err := bucket.rotate()
			if err != nil {
				return nil, err
			}
			for _, v := range volumes {
				logrus.WithField("action", "put").Debugf("Reference increase on volume %v", v)
				s.ref[v]++
			}
		}
		bucket.m.Unlock()
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

	bucket.MemoMap[request.Key] = entry
	bucket.MemoSize += request.Size
	s.TagMap[request.Tag] = &EntryMeta{
		Address: request.Address,
		Volume:  request.VolumeId,
		Offset:  request.Offset,
		Size:    request.Size,
	}
	if len(bucket.MemoMap) >= s.config.LayerKeyThreshold {
		volumes, err := bucket.rotate()
		if err != nil {
			return nil, err
		}
		for _, v := range volumes {
			logrus.WithField("action", "put").Debugf("Reference increase on volume %v", v)
			s.ref[v]++
		}
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
