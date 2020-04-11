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

	"oss/global"
	pm "oss/proto/metadata"
	ps "oss/proto/storage"

	"github.com/natefinch/atomic"
	"github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const executeTimeout = 2 * time.Second

// Config for metadata server
type Config struct {
	DumpFileName       string
	LayerKeyThreshold  int
	CompactThreshold   int
	TrashRateThreshold float64
	DumpTimeout        time.Duration
	HeartbeatTimeout   time.Duration
	CompactTimeout     time.Duration
	GCTimeout          time.Duration
}

// Server holds metadata information for index look up
type Server struct {
	pm.MetadataForStorageServer `json:"-"`
	pm.MetadataForProxyServer   `json:"-"`

	config        *Config
	m             sync.RWMutex
	ref           map[string]int64
	Root          string `json:"-"`
	Address       string `json:"-"`
	TagMap        map[string]*EntryMeta
	Bucket        map[string]*Bucket
	storageTimer  map[string]time.Time
	storageClerks map[string]*Clerk

	ClientId  int64
	CommandId int64
}

type Clerk struct {
	GroupId   string
	Addresses []string
	servers   []ps.StorageForMetadataClient
	LeaderId  int64
}

// NewMetadataServer returns a new metadata server
func NewMetadataServer(address string, root string, config *Config) *Server {
	s := &Server{
		m:             sync.RWMutex{},
		ref:           make(map[string]int64),
		config:        config,
		Root:          root,
		Address:       address,
		TagMap:        make(map[string]*EntryMeta),
		Bucket:        make(map[string]*Bucket),
		storageTimer:  make(map[string]time.Time),
		storageClerks: make(map[string]*Clerk),
		ClientId:      nrand(),
		CommandId:     0,
	}
	s.recover()
	go s.gcLoop()
	go s.dumpLoop()
	go s.compactLoop()
	go s.heartbeatLoop()
	return s
}

func (s *Server) recover() {
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
		for _, level := range bucket.SSTable.Layers {
			for _, layer := range level {
				for _, volume := range layer.Volumes {
					logrus.WithField("action", "recover").Debugf("Reference increase on volume %v", volume)
					s.ref[volume]++
				}
			}
		}
	}
}

func (s *Server) sendState(clerk *Clerk, request *ps.StateRequest) (*ps.StateResponse, error) {
	s.m.Lock()
	s.CommandId++
	request.CommandId = s.CommandId
	s.m.Unlock()
	deadline := time.Now().Add(executeTimeout)
	for {
		response, err := clerk.servers[clerk.LeaderId].State(context.Background(), request)
		if err == nil {
			return response, nil
		}
		st, _ := status.FromError(err)
		if st.Message() == global.ErrorWrongLeader {
			clerk.LeaderId = (clerk.LeaderId + 1) % int64(len(clerk.servers))
			if time.Now().After(deadline) {
				return nil, global.ErrorStorageConnection
			}
		} else if err != nil {
			return nil, err
		}
	}
}

func (s *Server) sendRotate(clerk *Clerk, request *ps.RotateRequest) (*ps.RotateResponse, error) {
	s.m.Lock()
	s.CommandId++
	request.CommandId = s.CommandId
	s.m.Unlock()
	deadline := time.Now().Add(executeTimeout)
	for {
		response, err := clerk.servers[clerk.LeaderId].Rotate(context.Background(), request)
		if err == nil {
			return response, nil
		}
		st, _ := status.FromError(err)
		if st.Message() == global.ErrorWrongLeader {
			clerk.LeaderId = (clerk.LeaderId + 1) % int64(len(clerk.servers))
			if time.Now().After(deadline) {
				return nil, global.ErrorStorageConnection
			}
		} else if err != nil {
			return nil, err
		}
	}
}

func (s *Server) sendMigrate(clerk *Clerk, request *ps.MigrateRequest) (*ps.MigrateResponse, error) {
	s.m.Lock()
	s.CommandId++
	request.CommandId = s.CommandId
	s.m.Unlock()
	deadline := time.Now().Add(executeTimeout)
	for {
		response, err := clerk.servers[clerk.LeaderId].Migrate(context.Background(), request)
		if err == nil {
			return response, nil
		}
		st, _ := status.FromError(err)
		if st.Message() == global.ErrorWrongLeader {
			clerk.LeaderId = (clerk.LeaderId + 1) % int64(len(clerk.servers))
			if time.Now().After(deadline) {
				return nil, global.ErrorStorageConnection
			}
		} else if err != nil {
			return nil, err
		}
	}
}

func (s *Server) sendDeleteVolume(clerk *Clerk, request *ps.DeleteVolumeRequest) (*ps.DeleteVolumeResponse, error) {
	s.m.Lock()
	s.CommandId++
	request.CommandId = s.CommandId
	s.m.Unlock()
	deadline := time.Now().Add(executeTimeout)
	for {
		response, err := clerk.servers[clerk.LeaderId].DeleteVolume(context.Background(), request)
		if err == nil {
			return response, nil
		}
		st, _ := status.FromError(err)
		if st.Message() == global.ErrorWrongLeader {
			clerk.LeaderId = (clerk.LeaderId + 1) % int64(len(clerk.servers))
			if time.Now().After(deadline) {
				return nil, global.ErrorStorageConnection
			}
		} else if err != nil {
			return nil, err
		}
	}
}

func (s *Server) recursiveCompact(b *Bucket, level int) {
	b.m.RLock()
	if level >= len(b.SSTable.Layers) || len(b.SSTable.Layers[level]) <= s.config.CompactThreshold {
		b.m.RUnlock()
		return
	}
	logrus.WithField("bucket", b.Name).Debugf("Perform compact on level %v", level)
	entries, err := b.mergeLayers(b.SSTable.Layers[level])
	if err != nil {
		logrus.WithError(err).Error("Merge layers failed")
		b.m.RUnlock()
		return
	}
	b.m.RUnlock()
	v2Size := make(map[string]int64)
	for _, entry := range entries {
		key := fmt.Sprintf("%v-%v", entry.Volume, entry.GroupId)
		v2Size[key] += entry.Size
		logrus.WithField("bucket", b.Name).Debugf("Add size %v on volume %v", entry.Size, key)
	}
	layers := [][]*Entry{entries}
	for key, size := range v2Size {
		var groupId string
		var volumeID int64
		fmt.Sscanf(key, "%v-%v", &volumeID, &groupId)
		s.m.RLock()
		clerk, ok := s.storageClerks[groupId]
		s.m.RUnlock()
		if !ok {
			logrus.WithField("group", groupId).Error("Storage client not found")
			return
		}
		state, err := s.sendState(clerk, &ps.StateRequest{
			VolumeId: volumeID,
			ClientId: s.ClientId,
		})
		if err != nil {
			logrus.WithError(err).Error("Get volume state failed")
			return
		}
		trashRate := 1.0 - float64(size)/float64(state.Size)

		if trashRate > s.config.TrashRateThreshold {
			logrus.WithField("volume", key).Debugf("Trashrate threshold exceeded, content %v, actual %v", size, state.Size)
			newEntries, err := s.compress(groupId, volumeID, entries)
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
		set[fmt.Sprintf("%v-%v", entry.Volume, entry.GroupId)] = struct{}{}
	}
	volumeIDs := make([]string, 0)
	for volumeID := range set {
		volumeIDs = append(volumeIDs, volumeID)
	}
	b.m.Lock()
	begin := b.SSTable.Layers[level][0].Begin
	end := b.SSTable.Layers[level][len(b.SSTable.Layers[level])-1].End
	compactedLayer := &Layer{
		Name:    fmt.Sprintf("%v-%v-%v", b.Name, begin, end),
		Volumes: volumeIDs,
		Begin:   begin,
		End:     end,
	}
	for _, layer := range b.SSTable.Layers[level] {
		b.deleteLayer(layer.Name)
		for _, volume := range layer.Volumes {
			logrus.WithField("action", "delete").Debugf("Reference decrease on volume %v", volume)
			s.ref[volume]--
		}
	}
	b.SSTable.clearLevel(level)
	layer, err := b.writeLayer(entries, volumeIDs, compactedLayer.Begin, compactedLayer.End)
	b.SSTable.pushLayer(layer, level+1)
	logrus.WithField("bucket", b.Name).Debugf("Push layer %v to level %v", layer.Name, level+1)
	for _, volumeID := range volumeIDs {
		logrus.WithField("action", "compact").Debugf("Referenece increase on volume %v", volumeID)
		s.ref[volumeID]++
	}
	if err != nil {
		logrus.WithError(err).Error("Write layer failed")
		b.m.Unlock()
		return
	}
	b.m.Unlock()
	s.recursiveCompact(b, level+1)
}

func (s *Server) compactLoop() {
	ticker := time.NewTicker(s.config.CompactTimeout)
	for {
		wg := sync.WaitGroup{}
		s.m.Lock()
		wg.Add(len(s.Bucket))
		for _, bucket := range s.Bucket {
			go func(b *Bucket) {
				defer wg.Done()
				s.recursiveCompact(b, 0)
			}(bucket)
		}
		s.m.Unlock()
		wg.Wait()
		<-ticker.C
	}
}

func (s *Server) compress(groupId string, volumeID int64, entries []*Entry) ([]*Entry, error) {
	logrus.WithFields(logrus.Fields{
		"group":    groupId,
		"volumeID": volumeID,
	}).Debug("Begin to compress volume")
	resultEntries := make([]*Entry, 0)
	size := int64(0)
	for _, entry := range entries {
		if entry.GroupId == groupId && entry.Volume == volumeID && !entry.Delete {
			resultEntries = append(resultEntries, entry)
			size += entry.Size
		}
	}
	if size == 0 {
		logrus.WithFields(logrus.Fields{
			"group":    groupId,
			"volumeID": volumeID,
		}).Debug("Remove empty volume")
		return []*Entry{}, nil
	}
	clerk, ok := s.storageClerks[groupId]
	if !ok {
		return nil, status.Error(codes.NotFound, "no such storage client")
	}
	_, err := s.sendRotate(clerk, &ps.RotateRequest{
		ClientId: s.ClientId,
	})
	if err != nil {
		return nil, status.Error(codes.NotFound, "create new volume failed")
	}
	for _, entry := range resultEntries {
		response, err := s.sendMigrate(clerk, &ps.MigrateRequest{
			VolumeId: entry.Volume,
			Offset:   entry.Offset,
			ClientId: s.ClientId,
		})
		if err != nil {
			return nil, err
		}
		entry.Volume = response.VolumeId
		entry.Offset = response.Offset
	}
	return resultEntries, nil
}

func (s *Server) heartbeatLoop() {
	ticker := time.NewTicker(s.config.HeartbeatTimeout)
	for {
		for group, t := range s.storageTimer {
			if t.Add(s.config.HeartbeatTimeout).Before(time.Now()) {
				logrus.WithField("group", group).Warn("Close expired storage connenction")
				delete(s.storageTimer, group)
				delete(s.storageClerks, group)
			}
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

func (s *Server) gcLoop() {
	ticker := time.NewTicker(s.config.GCTimeout)
	for {
		s.m.RLock()
		refs := s.ref
		s.m.RUnlock()
		for volume, ref := range refs {
			if ref > 0 {
				continue
			}
			var groupId string
			var volumeID int64
			fmt.Sscanf(volume, "%v-%v", &volumeID, &groupId)
			found := func() bool {
				for _, bucket := range s.Bucket {
					for _, entry := range bucket.MemoTree.toList() {
						if entry.GroupId == groupId && entry.Volume == volumeID {
							return true
						}
					}
				}
				return false
			}()
			if found {
				continue
			}
			s.m.Lock()
			delete(s.ref, volume)
			logrus.WithField("volume", volume).Debug("Delete zero referenced volume")
			clerk, ok := s.storageClerks[groupId]
			if !ok {
				logrus.WithField("group", groupId).Error("Storage client not found")
				s.m.Unlock()
				continue
			}
			for tag, entry := range s.TagMap {
				if entry.Volume == volumeID {
					delete(s.TagMap, tag)
				}
			}
			s.m.Unlock()
			_, err := s.sendDeleteVolume(clerk, &ps.DeleteVolumeRequest{
				VolumeId: volumeID,
				ClientId: s.ClientId,
			})
			if err != nil {
				logrus.WithError(err).Error("GC useless volume failed")
			} else {
				logrus.Debug("GC useless volume succeeded")
			}
		}
		<-ticker.C
	}
}

func (s *Server) searchEntry(bucket *Bucket, key string) (*Entry, error) {
	entry, ok := bucket.MemoTree.get(key)
	if ok {
		return entry, nil
	}
	for _, level := range bucket.SSTable.Layers {
		for i := len(level) - 1; i >= 0; i-- {
			layer := level[i]
			entryList, err := bucket.readLayer(layer.Name)
			if err != nil {
				logrus.WithError(err).Errorf("Get layer %v content failed", layer.Name)
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
	}
	return nil, nil
}

func (s *Server) ListBucket(ctx context.Context, request *pm.ListBucketRequest) (*pm.ListBucketResposne, error) {
	s.m.RLock()
	defer s.m.RUnlock()
	buckets := make([]string, 0)
	for bucket := range s.Bucket {
		buckets = append(buckets, bucket)
	}
	return &pm.ListBucketResposne{
		Buckets: buckets,
	}, nil
}

// Heartbeat deals heartbeat requests from storage servers
func (s *Server) Heartbeat(ctx context.Context, request *pm.HeartbeatRequest) (*pm.HeartbeatResponse, error) {
	logrus.WithField("groupID", request.Group.GroupId).Debug("Received heartbeat")
	group := request.Group
	s.m.Lock()
	defer s.m.Unlock()
	_, ok := s.storageClerks[group.GroupId]

	if !ok {
		storageClients := make([]ps.StorageForMetadataClient, 0)
		for _, address := range group.Addresses {
			connection, err := grpc.Dial(address, grpc.WithInsecure())
			if err != nil {
				return nil, err
			}
			storageClients = append(storageClients, ps.NewStorageForMetadataClient(connection))
		}
		s.storageClerks[group.GroupId] = &Clerk{
			GroupId:   group.GroupId,
			Addresses: group.Addresses,
			servers:   storageClients,
			LeaderId:  request.LeaderId,
		}
		logrus.WithField("group", group.GroupId).Info("Connect to new storage server group")
	}
	s.storageTimer[group.GroupId] = time.Now()
	return &pm.HeartbeatResponse{}, nil
}

// CreateBucket handles bucket creation request
func (s *Server) CreateBucket(ctx context.Context, request *pm.CreateBucketRequest) (*pm.CreateBucketResponse, error) {
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
		Root:       s.Root,
		Name:       bucketName,
		MemoTree:   new(RBTree),
		SSTable:    newLSTM(),
		MemoSize:   0,
		CreateTime: time.Now().Unix(),
	}
	s.Bucket[bucketName] = bucket
	return &pm.CreateBucketResponse{}, nil
}

// DeleteBucket handles bucket deletion request
func (s *Server) DeleteBucket(ctx context.Context, request *pm.DeleteBucketRequest) (*pm.DeleteBucketResponse, error) {
	bucketName := request.Bucket
	s.m.Lock()
	defer s.m.Unlock()
	b, ok := s.Bucket[bucketName]
	if !ok {
		return nil, status.Error(codes.NotFound, "bucket not found")
	}
	logrus.WithField("bucket", bucketName).Debug("Delete bucket")
	for _, level := range b.SSTable.Layers {
		for _, layer := range level {
			s.ref[layer.Name]--
			name := fmt.Sprintf("%v-%v-%v", b.Name, layer.Begin, layer.End)
			err := os.Remove(path.Join(b.Root, name))
			if err != nil {
				logrus.WithField("bucket", bucketName).Error("delete layer file failed")
			}
		}
	}

	delete(s.Bucket, bucketName)
	return &pm.DeleteBucketResponse{}, nil
}

// CheckMeta checks the existence of object metadata before putting object to avoid data duplicity
func (s *Server) CheckMeta(ctx context.Context, request *pm.CheckMetaRequest) (*pm.CheckMetaResponse, error) {
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
			Name:       request.Name,
			GroupId:    meta.GroupId,
			Volume:     meta.Volume,
			Offset:     meta.Offset,
			Size:       meta.Size,
			CreateTime: time.Now().Unix(),
			Delete:     false,
		}
		bucket.MemoTree.put(request.Key, entry)
		if bucket.MemoTree.size() >= s.config.LayerKeyThreshold {
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
			Group:   nil,
		}, nil
	}
	s.m.RLock()
	defer s.m.RUnlock()
	if len(s.storageClerks) != 0 {
		for _, group := range s.storageClerks {
			return &pm.CheckMetaResponse{
				Existed: false,
				Group: &pm.Group{
					GroupId:   group.GroupId,
					Addresses: group.Addresses,
				},
			}, nil
		}
	}
	return nil, status.Error(codes.Unavailable, "no storage cluster available")
}

// PutMeta adds an key value entry to index structure
func (s *Server) PutMeta(ctx context.Context, request *pm.PutMetaRequest) (*pm.PutMetaResponse, error) {
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
		Name:       request.Name,
		GroupId:    request.GroupId,
		Volume:     request.VolumeId,
		Offset:     request.Offset,
		Size:       request.Size,
		CreateTime: time.Now().Unix(),
	}

	bucket.MemoTree.put(request.Key, entry)
	bucket.MemoSize += request.Size
	s.TagMap[request.Tag] = &EntryMeta{
		GroupId: request.GroupId,
		Volume:  request.VolumeId,
		Offset:  request.Offset,
		Size:    request.Size,
	}
	if bucket.MemoTree.size() >= s.config.LayerKeyThreshold {
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

// GetMeta handles get metadata request
func (s *Server) GetMeta(ctx context.Context, request *pm.GetMetaRequest) (*pm.GetMetaResponse, error) {
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
		Name: entry.Name,
		Group: &pm.Group{
			GroupId:   entry.GroupId,
			Addresses: s.storageClerks[entry.GroupId].Addresses,
		},
		VolumeId:   int64(entry.Volume),
		Offset:     int64(entry.Offset),
		Size:       int64(entry.Size),
		CreateTime: int64(entry.CreateTime),
	}, nil
}

func (s *Server) ListObject(ctx context.Context, request *pm.ListObjectRequest) (*pm.ListObjectResponse, error) {
	s.m.RLock()
	bucket, ok := s.Bucket[request.Bucket]
	if !ok {
		s.m.RUnlock()
		return nil, status.Error(codes.NotFound, "bucket not exist")
	}
	bucket.m.RLock()
	defer bucket.m.RUnlock()
	s.m.RUnlock()
	entryMatrix := make([][]*Entry, 0)
	for _, level := range bucket.SSTable.Layers {
		for _, layer := range level {
			entries, err := bucket.readLayer(layer.Name)
			if err != nil {
				logrus.WithError(err).Errorf("Range layer %v failed", layer.Name)
				return nil, err
			}
			if len(entries) > 0 {
				entryMatrix = append(entryMatrix, entries)
			}
		}
	}
	slice := bucket.MemoTree.toList()
	if len(slice) > 0 {
		entryMatrix = append(entryMatrix, slice)
	}
	objects := make([]*pm.Metadata, 0)
	for _, entry := range mergeEntryMatrix(entryMatrix) {
		objects = append(objects, &pm.Metadata{
			Key:         entry.Key,
			Name:        entry.Name,
			Size:        entry.Size,
			CreatedTime: entry.CreateTime,
		})
	}
	return &pm.ListObjectResponse{
		Objects: objects,
	}, nil
}

func (s *Server) RangeObject(ctx context.Context, request *pm.RangeObjectRequest) (*pm.RangeObjectResponse, error) {
	s.m.RLock()
	bucket, ok := s.Bucket[request.Bucket]
	if !ok {
		s.m.RUnlock()
		return nil, status.Error(codes.NotFound, "bucket not exist")
	}
	bucket.m.RLock()
	defer bucket.m.RUnlock()
	s.m.RUnlock()
	entryMatrix := make([][]*Entry, 0)
	for _, level := range bucket.SSTable.Layers {
		for _, layer := range level {
			entries, err := bucket.readLayer(layer.Name)
			if err != nil {
				logrus.WithError(err).Errorf("Range layer %v failed", layer.Name)
				return nil, err
			}
			slice := entrySliceRange(entries, request.From, request.To)
			if len(slice) > 0 {
				entryMatrix = append(entryMatrix, slice)
			}
		}
	}
	slice := bucket.MemoTree.getRange(request.From, request.To)
	if len(slice) > 0 {
		entryMatrix = append(entryMatrix, slice)
	}
	keys := make([]string, 0)
	for _, entry := range mergeEntryMatrix(entryMatrix) {
		keys = append(keys, entry.Key)
	}
	return &pm.RangeObjectResponse{
		Key: keys,
	}, nil
}

// DeleteMeta handles delete metadata request
func (s *Server) DeleteMeta(ctx context.Context, request *pm.DeleteMetaRequest) (*pm.DeleteMetaResponse, error) {
	s.m.RLock()
	bucket, ok := s.Bucket[request.Bucket]
	if !ok {
		s.m.RUnlock()
		return nil, status.Error(codes.NotFound, "bucket not exist")
	}
	bucket.m.Lock()
	defer bucket.m.Unlock()
	s.m.RUnlock()
	e, ok := bucket.MemoTree.get(request.Key)
	if ok {
		e.Delete = true
	} else {
		entry, err := s.searchEntry(bucket, request.Key)
		if err != nil {
			return nil, err
		}
		if entry == nil || entry.Delete {
			return nil, status.Error(codes.NotFound, "object metadata not found")
		}
		entry.Delete = true
		bucket.MemoTree.put(request.Key, entry)
	}
	return &pm.DeleteMetaResponse{}, nil
}
