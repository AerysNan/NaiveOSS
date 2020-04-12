package storage

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/gob"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net"
	"os"
	"path"
	"sync"
	"time"

	"oss/global"
	pm "oss/proto/metadata"
	pr "oss/proto/raft"
	ps "oss/proto/storage"
	"oss/raft"

	"github.com/golang/protobuf/ptypes"
	"github.com/klauspost/reedsolomon"
	"github.com/natefinch/atomic"
	"github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var enc reedsolomon.Encoder

const ExecuteTimeout = 500 * time.Millisecond

type Response struct {
	op       *pr.Op
	response interface{}
	err      error
}

// Config for storage server
type Config struct {
	VolumeMaxSize     int64
	DumpFileName      string
	DataShard         int
	ParityShard       int
	BSDNum            int
	GroupID           string
	DumpTimeout       time.Duration
	HeartbeatTimeout  time.Duration
	ValidBlobTimeout  time.Duration
	Address           string
	PeerAddresses     []string
	RaftAddress       string
	RaftPeerAddresses []string
	Root              string
}

// Server represents storage server for storing object data
type Server struct {
	ps.StorageForProxyServer
	ps.StorageForMetadataServer
	metadataClient pm.MetadataForStorageClient

	m             sync.RWMutex
	rf            *raft.Raft
	config        *Config
	maxraftstate  int
	Address       string `json:"-"`
	Root          string `json:"-"`
	BSDs          map[int64]*BSD
	Blobs         map[string]*Blob
	Volumes       map[int64]*Volume
	CurrentVolume int64

	notifyChs           map[int]chan Response
	commandIds          map[int64]int64
	applyCh             chan pr.Message
	persister           *raft.Persister
	appliedRaftLogIndex int
}

func NewStorageServer(metadataClient pm.MetadataForStorageClient, config *Config) *Server {
	encoderInit(config)
	persister := raft.MakePersister(config.Root)
	storageServer := &Server{
		metadataClient: metadataClient,
		m:              sync.RWMutex{},
		config:         config,
		maxraftstate:   1 << 20,
		Address:        config.Address,
		Root:           config.Root,
		Blobs:          make(map[string]*Blob),
		BSDs:           make(map[int64]*BSD),
		Volumes:        make(map[int64]*Volume),
		CurrentVolume:  0,

		notifyChs:           make(map[int]chan Response),
		commandIds:          make(map[int64]int64),
		applyCh:             make(chan pr.Message),
		persister:           persister,
		appliedRaftLogIndex: 0,
	}
	storageServer.rf = raft.Make(make([]pr.RaftClient, len(config.RaftPeerAddresses)), -1, storageServer.persister, storageServer.applyCh)
	go storageServer.startRaftServer()
	me, clients := storageServer.buildRaftConnection()
	storageServer.rf.SetConnections(me, clients)
	go storageServer.rf.StartTimer()
	storageServer.rf.SetLogLevel(logrus.ErrorLevel)

	storageServer.recover()
	storageServer.bsdInit()
	storageServer.addVolume()
	storageServer.installSnapshot(persister.ReadSnapshot())
	go storageServer.dumpLoop()
	go storageServer.heartbeatLoop()
	go storageServer.dispatch()
	return storageServer
}

func (s *Server) startRaftServer() {
	server := grpc.NewServer()
	listen, err := net.Listen("tcp", s.config.RaftAddress)
	if err != nil {
		logrus.WithError(err).Fatal("Listen port failed")
	}
	pr.RegisterRaftServer(server, s.rf)
	logrus.WithField("address", s.config.RaftAddress).Info("Raft server started")
	if err := server.Serve(listen); err != nil {
		logrus.WithError(err).Fatal("Server failed")
	}
}

func (s *Server) buildRaftConnection() (int, []pr.RaftClient) {
	me := -1
	for i, address := range s.config.RaftPeerAddresses {
		if address == s.config.RaftAddress {
			me = i
			break
		}
	}
	if me == -1 {
		return -1, nil
	}
	clients := make([]pr.RaftClient, len(s.config.PeerAddresses))
	for i, address := range s.config.RaftPeerAddresses {
		if i == me {
			continue
		}
		for {
			connection, err := grpc.Dial(address, grpc.WithInsecure(), grpc.WithBlock())
			if err != nil {
				logrus.WithError(err).Warnf("Failed to connect to %v", address)
				time.Sleep(time.Second)
			} else {
				logrus.WithField("id", i).Infof("Raft client successfully connected to %v", address)
				clients[i] = pr.NewRaftClient(connection)
				break
			}
		}
	}
	return me, clients
}

func (s *Server) dispatch() {
	for message := range s.applyCh {
		logrus.Debugf("Received message %v", message)
		if !message.CommandValid {
			continue
		}
		op := message.Command
		if op.Type == pr.Type_SNAPSHOT {
			var args pr.SnapshotRequest
			err := ptypes.UnmarshalAny(op.Args, &args)
			if err != nil {
				logrus.WithError(err).Warn("Unmarshal any type failed")
				continue
			}
			s.installSnapshot(args.Data)
			continue
		}
		s.m.Lock()
		if s.isDuplicateRequest(op.ClientId, op.CommandId) {
			logrus.Debugf("Find duplicate request recorded command id %v actual command id %v", s.commandIds[op.ClientId], op.CommandId)
			s.m.Unlock()
			continue
		}
		s.m.Unlock()
		result := Response{
			op: op,
		}
		switch op.Type {
		case pr.Type_MIGRATE:
			result.response, result.err = s.applyMigrate(op)
		case pr.Type_ROTATE:
			result.response, result.err = s.applyRotate(op)
		case pr.Type_DELETE:
			result.response, result.err = s.applyDeleteVolume(op)
		case pr.Type_CREATE:
			result.response, result.err = s.applyCreate(op)
		case pr.Type_PUT:
			result.response, result.err = s.applyPut(op)
		case pr.Type_CONFIRM:
			result.response, result.err = s.applyConfirm(op)
		}
		s.m.Lock()
		s.commandIds[op.ClientId] = op.CommandId
		s.appliedRaftLogIndex = int(message.CommandIndex)
		logrus.Debugf("Write notify channel index %v", message.CommandIndex)
		if ch, ok := s.notifyChs[int(message.CommandIndex)]; ok {
			ch <- result
		}
		s.m.Unlock()
	}
}

func (s *Server) applyMigrate(op *pr.Op) (*ps.MigrateResponse, error) {
	logrus.Debugf("Apply migrate")
	var request ps.MigrateRequest
	err := ptypes.UnmarshalAny(op.Args, &request)
	if err != nil {
		return nil, err
	}
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

func (s *Server) applyRotate(op *pr.Op) (*ps.RotateResponse, error) {
	logrus.Debugf("Apply rotate")
	var request ps.MigrateRequest
	err := ptypes.UnmarshalAny(op.Args, &request)
	if err != nil {
		return nil, err
	}
	s.m.Lock()
	defer s.m.Unlock()
	s.addVolume()
	return &ps.RotateResponse{}, nil
}

func (s *Server) applyDeleteVolume(op *pr.Op) (*ps.DeleteVolumeResponse, error) {
	logrus.Debugf("Apply delete volume")
	var request ps.MigrateRequest
	err := ptypes.UnmarshalAny(op.Args, &request)
	if err != nil {
		return nil, err
	}
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

func (s *Server) applyCreate(op *pr.Op) (*ps.CreateResponse, error) {
	logrus.Debugf("Apply create")
	var request ps.CreateRequest
	err := ptypes.UnmarshalAny(op.Args, &request)
	if err != nil {
		return nil, err
	}
	name := path.Join(s.Root, fmt.Sprintf("%s.tmp", request.Id))
	_, err = os.Create(name)
	if err != nil {
		logrus.WithError(err).Errorf("Create file %v failed", name)
		return nil, status.Error(codes.Internal, "create tmp file failed")
	}
	s.m.Lock()
	s.Blobs[request.Id] = NewBlob(request.Id, request.Tag)
	s.m.Unlock()
	return &ps.CreateResponse{}, nil
}

func (s *Server) applyPut(op *pr.Op) (*ps.PutResponse, error) {
	logrus.Debugf("Apply put")
	var request ps.PutRequest
	err := ptypes.UnmarshalAny(op.Args, &request)
	if err != nil {
		return nil, err
	}
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

func (s *Server) applyConfirm(op *pr.Op) (*ps.ConfirmResponse, error) {
	logrus.Debugf("Apply confirm")
	var request ps.ConfirmRequest
	err := ptypes.UnmarshalAny(op.Args, &request)
	if err != nil {
		return nil, err
	}
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

func (s *Server) installSnapshot(snapshot []byte) {
	s.m.Lock()
	defer s.m.Unlock()
	if snapshot == nil {
		return
	}
	r := bytes.NewBuffer(snapshot)
	d := gob.NewDecoder(r)
	if d.Decode(&s.Blobs) != nil ||
		d.Decode(&s.Volumes) != nil ||
		d.Decode(&s.CurrentVolume) != nil ||
		d.Decode(&s.commandIds) != nil {
		logrus.Warnf("Install snapshot failed")
	}
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
		_, isLeader := s.rf.GetState()
		if isLeader {
			ctx := context.Background()
			_, err := s.metadataClient.Heartbeat(ctx, &pm.HeartbeatRequest{
				Group: &pm.Group{
					GroupId:   s.config.GroupID,
					Addresses: s.config.PeerAddresses,
				},
			})
			if err != nil {
				logrus.WithError(err).Error("Heartbeat failed")
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

func (s *Server) isDuplicateRequest(clientId int64, requestId int64) bool {
	appliedRequestId, ok := s.commandIds[clientId]
	return ok && requestId <= appliedRequestId
}

func (s *Server) execute(op *pr.Op, timeout time.Duration) (*Response, bool) {
	index, _, isLeader := s.rf.Start(op)
	if !isLeader {
		return nil, true
	}
	logrus.Debugf("Execute command %v index %v command id %v", op.Type.String(), index, op.CommandId)
	if s.maxraftstate >= 0 && s.rf.RaftStateSize() >= s.maxraftstate {
		s.snapshot()
	}
	s.m.Lock()
	if _, ok := s.notifyChs[index]; !ok {
		s.notifyChs[index] = make(chan Response, 1)
	}
	ch := s.notifyChs[index]
	logrus.Debugf("Sleep for notify channel index %v", index)
	s.m.Unlock()
	var wrongLeader bool
	var result Response
	select {
	case result = <-ch:
		logrus.Debugf("Read result op %v from notify channel index %v", result.op.String(), index)
		wrongLeader = !(result.op.ClientId == op.ClientId && result.op.CommandId == op.CommandId)
	case <-time.After(timeout):
		s.m.Lock()
		wrongLeader = !s.isDuplicateRequest(op.ClientId, op.CommandId)
		if !wrongLeader {
			logrus.Debugf("Timout due to duplicate request index %v", index)
			result.err = global.ErrorDuplicateRequest
		}
		s.m.Unlock()
	}
	s.m.Lock()
	delete(s.notifyChs, index)
	s.m.Unlock()
	logrus.Debugf("Execute command index %v type %v finished", index, op.Type.String())
	return &result, wrongLeader
}

func (s *Server) snapshot() {
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	s.m.Lock()
	_ = e.Encode(s.Blobs)
	_ = e.Encode(s.Volumes)
	_ = e.Encode(s.CurrentVolume)
	_ = e.Encode(s.commandIds)
	appliedRaftLogIndex := s.appliedRaftLogIndex
	s.m.Unlock()
	s.rf.ReplaceLogWithSnapshot(appliedRaftLogIndex, w.Bytes())
}

func (s *Server) CheckBlob(ctx context.Context, request *ps.CheckBlobRequest) (*ps.CheckBlobResponse, error) {
	any, err := ptypes.MarshalAny(request)
	if err != nil {
		return nil, err
	}
	_, wrongLeader := s.execute(&pr.Op{
		Type:      pr.Type_CHECK,
		ClientId:  request.ClientId,
		CommandId: request.CommandId,
		Args:      any,
	}, ExecuteTimeout)
	if wrongLeader {
		return nil, status.Error(codes.Unauthenticated, global.ErrorWrongLeader)
	}
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
	any, err := ptypes.MarshalAny(request)
	if err != nil {
		return nil, err
	}
	_, wrongLeader := s.execute(&pr.Op{
		Type:      pr.Type_STATE,
		ClientId:  request.ClientId,
		CommandId: request.CommandId,
		Args:      any,
	}, ExecuteTimeout)
	if wrongLeader {
		return nil, status.Error(codes.Unauthenticated, global.ErrorWrongLeader)
	}
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
	any, err := ptypes.MarshalAny(request)
	if err != nil {
		return nil, err
	}
	result, wrongLeader := s.execute(&pr.Op{
		Type:      pr.Type_MIGRATE,
		ClientId:  request.ClientId,
		CommandId: request.CommandId,
		Args:      any,
	}, ExecuteTimeout)
	if wrongLeader {
		return nil, status.Error(codes.Unauthenticated, global.ErrorWrongLeader)
	}
	if result.err != nil {
		return nil, result.err
	}
	return result.response.(*ps.MigrateResponse), nil
}

// TODO
// Rotate converts a writable volume to read only and create a new volume
func (s *Server) Rotate(ctx context.Context, request *ps.RotateRequest) (*ps.RotateResponse, error) {
	any, err := ptypes.MarshalAny(request)
	if err != nil {
		return nil, err
	}
	result, wrongLeader := s.execute(&pr.Op{
		Type:      pr.Type_ROTATE,
		ClientId:  request.ClientId,
		CommandId: request.CommandId,
		Args:      any,
	}, ExecuteTimeout)
	if wrongLeader {
		return nil, status.Error(codes.Unauthenticated, global.ErrorWrongLeader)
	}
	if result.err != nil {
		return nil, result.err
	}
	return result.response.(*ps.RotateResponse), nil
}

// DeleteVolume handles delete volume request
func (s *Server) DeleteVolume(ctx context.Context, request *ps.DeleteVolumeRequest) (*ps.DeleteVolumeResponse, error) {
	any, err := ptypes.MarshalAny(request)
	if err != nil {
		return nil, err
	}
	result, wrongLeader := s.execute(&pr.Op{
		Type:      pr.Type_DELETE,
		ClientId:  request.ClientId,
		CommandId: request.CommandId,
		Args:      any,
	}, ExecuteTimeout)
	if wrongLeader {
		return nil, status.Error(codes.Unauthenticated, global.ErrorWrongLeader)
	}
	if result.err != nil {
		return nil, result.err
	}
	return result.response.(*ps.DeleteVolumeResponse), nil
}

func (s *Server) Get(ctx context.Context, request *ps.GetRequest) (*ps.GetResponse, error) {
	any, err := ptypes.MarshalAny(request)
	if err != nil {
		return nil, err
	}
	_, wrongLeader := s.execute(&pr.Op{
		Type:      pr.Type_GET,
		ClientId:  request.ClientId,
		CommandId: request.CommandId,
		Args:      any,
	}, ExecuteTimeout)
	if wrongLeader {
		return nil, status.Error(codes.Unauthenticated, global.ErrorWrongLeader)
	}
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
	any, err := ptypes.MarshalAny(request)
	if err != nil {
		return nil, err
	}
	result, wrongLeader := s.execute(&pr.Op{
		Type:      pr.Type_CREATE,
		ClientId:  request.ClientId,
		CommandId: request.CommandId,
		Args:      any,
	}, ExecuteTimeout)
	if wrongLeader {
		return nil, status.Error(codes.Unauthenticated, global.ErrorWrongLeader)
	}
	if result.err != nil {
		return nil, result.err
	}
	return result.response.(*ps.CreateResponse), nil
}

func (s *Server) Put(ctx context.Context, request *ps.PutRequest) (*ps.PutResponse, error) {
	any, err := ptypes.MarshalAny(request)
	if err != nil {
		return nil, err
	}
	result, wrongLeader := s.execute(&pr.Op{
		Type:      pr.Type_PUT,
		ClientId:  request.ClientId,
		CommandId: request.CommandId,
		Args:      any,
	}, ExecuteTimeout)
	if wrongLeader {
		return nil, status.Error(codes.Unauthenticated, global.ErrorWrongLeader)
	}
	if result.err != nil {
		return nil, result.err
	}
	return result.response.(*ps.PutResponse), nil
}

func (s *Server) Confirm(ctx context.Context, request *ps.ConfirmRequest) (*ps.ConfirmResponse, error) {
	any, err := ptypes.MarshalAny(request)
	if err != nil {
		return nil, err
	}
	result, wrongLeader := s.execute(&pr.Op{
		Type:      pr.Type_CONFIRM,
		ClientId:  request.ClientId,
		CommandId: request.CommandId,
		Args:      any,
	}, ExecuteTimeout)
	if wrongLeader {
		return nil, status.Error(codes.Unauthenticated, global.ErrorWrongLeader)
	}
	if result.err != nil {
		return nil, result.err
	}
	return result.response.(*ps.ConfirmResponse), nil
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
		Size:     size,
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
