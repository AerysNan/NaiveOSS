package proxy

import (
	"context"
	"crypto/sha256"
	"fmt"
	"io/ioutil"
	"net/http"
	"sync"

	pm "oss/proto/metadata"
	ps "oss/proto/storage"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var maxStorageConnection = 10

type ProxyServer struct {
	http.Handler
	metadataClient pm.MetadataForProxyClient
	storageClients map[string]*grpc.ClientConn
	m              *sync.RWMutex
	address        string
}

func NewProxyServer(address string, metadataClient pm.MetadataForProxyClient) *ProxyServer {
	return &ProxyServer{
		address:        address,
		m:              new(sync.RWMutex),
		metadataClient: metadataClient,
		storageClients: make(map[string]*grpc.ClientConn),
	}
}

func (s *ProxyServer) createBucket(w http.ResponseWriter, r *http.Request) {
	p, err := checkParameter(r, []string{"bucket"})
	if err != nil {
		writeError(w, err)
		return
	}
	bucket := p["bucket"]
	_, err = s.metadataClient.CreateBucket(context.Background(), &pm.CreateBucketRequest{
		Bucket: bucket,
	})
	if err != nil {
		writeError(w, err)
		return
	}
	writeResponse(w, nil)
}

func (s *ProxyServer) putObject(w http.ResponseWriter, r *http.Request) {
	p, err := checkParameter(r, []string{"bucket", "key", "tag"})
	if err != nil {
		writeError(w, err)
		return
	}
	bucket, key, rtag := p["bucket"], p["key"], p["tag"]
	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		writeError(w, err)
		return
	}
	ctx := context.Background()
	tag := fmt.Sprintf("%x", sha256.Sum256(body))
	if tag != rtag {
		writeError(w, status.Error(codes.Unknown, "Data transmission error"))
		return
	}
	response, err := s.metadataClient.CheckMeta(ctx, &pm.CheckMetaRequest{
		Bucket: bucket,
		Key:    key,
		Tag:    tag,
	})
	if err != nil {
		writeError(w, err)
		return
	}
	if response.Existed {
		writeResponse(w, nil)
		return
	}
	address := response.Address
	s.m.Lock()
	connection, ok := s.storageClients[address]
	if !ok {
		connection, err = grpc.Dial(address, grpc.WithInsecure())
		if err != nil {
			writeError(w, err)
			s.m.Unlock()
			return
		}
		if len(s.storageClients) >= maxStorageConnection {
			for k := range s.storageClients {
				_ = s.storageClients[k].Close()
				delete(s.storageClients, k)
				s.storageClients[address] = connection
				break
			}
		}
	}
	s.m.Unlock()
	storageClient := ps.NewStorageForProxyClient(connection)
	ctx = context.Background()
	putResponse, err := storageClient.Put(ctx, &ps.PutRequest{
		Body: string(body),
		Tag:  tag,
	})
	if err != nil {
		writeError(w, err)
		return
	}
	_, err = s.metadataClient.PutMeta(ctx, &pm.PutMetaRequest{
		Bucket:   bucket,
		Key:      key,
		Tag:      tag,
		Address:  address,
		VolumeId: putResponse.VolumeId,
		Offset:   putResponse.Offset,
		Size:     int64(len(body)),
	})
	if err != nil {
		writeError(w, err)
		return
	}
	writeResponse(w, nil)
}

func (s *ProxyServer) getObject(w http.ResponseWriter, r *http.Request) {
	p, err := checkParameter(r, []string{"bucket", "key"})
	if err != nil {
		writeError(w, err)
		return
	}
	bucket, key := p["bucket"], p["key"]
	ctx := context.Background()
	getMetaResponse, err := s.metadataClient.GetMeta(ctx, &pm.GetMetaRequest{
		Bucket: bucket,
		Key:    key,
	})
	if err != nil {
		writeError(w, err)
		return
	}
	address := getMetaResponse.Address
	s.m.Lock()
	connection, ok := s.storageClients[address]
	if !ok {
		connection, err = grpc.Dial(address, grpc.WithInsecure())
		if err != nil {
			writeError(w, err)
			s.m.Unlock()
			return
		}
		if len(s.storageClients) >= maxStorageConnection {
			for k := range s.storageClients {
				_ = s.storageClients[k].Close()
				delete(s.storageClients, k)
				s.storageClients[address] = connection
				break
			}
		}
	}
	s.m.Unlock()
	storageClient := ps.NewStorageForProxyClient(connection)
	ctx = context.Background()
	getResponse, err := storageClient.Get(ctx, &ps.GetRequest{
		VolumeId: getMetaResponse.VolumeId,
		Offset:   getMetaResponse.Offset,
	})
	if err != nil {
		writeError(w, err)
		return
	}
	writeResponse(w, []byte(getResponse.Body))
}

func (s *ProxyServer) deleteObject(w http.ResponseWriter, r *http.Request) {
	p, err := checkParameter(r, []string{"bucket", "key"})
	if err != nil {
		writeError(w, err)
		return
	}
	bucket, key := p["bucket"], p["key"]
	_, err = s.metadataClient.DeleteMeta(context.Background(), &pm.DeleteMetaRequest{
		Bucket: bucket,
		Key:    key,
	})
	if err != nil {
		writeError(w, err)
		return
	}
	writeResponse(w, nil)
}

func (s *ProxyServer) getObjectMeta(w http.ResponseWriter, r *http.Request) {
	p, err := checkParameter(r, []string{"bucket", "key"})
	if err != nil {
		writeError(w, err)
		return
	}
	bucket, key := p["bucket"], p["key"]
	response, err := s.metadataClient.GetMeta(context.Background(), &pm.GetMetaRequest{
		Bucket: bucket,
		Key:    key,
	})
	if err != nil {
		writeError(w, err)
		return
	}
	writeResponse(w, []byte(response.String()))
}
