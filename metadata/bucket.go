package metadata

import (
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"sync"

	"github.com/sirupsen/logrus"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type Entry struct {
	Key        string
	Tag        string
	Address    string
	Volume     int64
	Offset     int64
	Size       int64
	CreateTime int64
	Delete     bool
}

type EntryList []*Entry

func (l EntryList) Len() int {
	return len(l)
}

func (l EntryList) Less(i int, j int) bool {
	return l[i].Key < l[j].Key
}

func (l EntryList) Swap(i int, j int) {
	l[i], l[j] = l[j], l[i]
}

type EntryMeta struct {
	Address string
	Volume  int64
	Offset  int64
	Size    int64
}

type Layer struct {
	Name    string
	Size    int64
	Volumes []int64
}

type Bucket struct {
	m          sync.RWMutex
	Name       string            // bucket name
	MemoMap    map[string]*Entry // key -> entry
	SSTable    []*Layer          // read only layer list
	MemoSize   int64             // size of mempmap
	CreateTime int64             // create timestamp
}

func (b *Bucket) rotate() ([]int64, error) {
	entryList := make(EntryList, 0)
	volumeSet := make(map[int64]struct{})
	for _, v := range b.MemoMap {
		entryList = append(entryList, v)
		volumeSet[v.Volume] = struct{}{}
	}
	volumes := make([]int64, 0)
	for volume := range volumeSet {
		volumes = append(volumes, volume)
	}
	b.MemoMap = make(map[string]*Entry)
	sort.Sort(entryList)
	err := b.writeLayer(entryList, volumes)
	if err != nil {
		return nil, err
	}
	return volumes, nil
}

func (b *Bucket) writeLayer(entryList []*Entry, volumes []int64) error {
	bytes, err := json.Marshal(entryList)
	if err != nil {
		logrus.WithError(err).Warn("Marshal JSON failed")
		return status.Error(codes.Internal, "marshal JSON failed")
	}
	name := fmt.Sprintf("%v-%v", b.Name, len(b.SSTable))
	file, err := os.Create(name)
	if err != nil {
		logrus.WithField("bucket", b.Name).WithError(err).Warn("Create layer file failed")
		return status.Error(codes.Internal, "create layer file failed")
	}
	defer file.Close()
	_, err = file.Write(bytes)
	if err != nil {
		logrus.WithField("bucket", b.Name).WithError(err).Warn("Write layer file failed")
		return status.Error(codes.Internal, "write layer file failed")
	}
	layer := &Layer{
		Name:    name,
		Size:    int64(len(bytes)),
		Volumes: volumes,
	}
	b.SSTable = append(b.SSTable, layer)
	return nil
}
