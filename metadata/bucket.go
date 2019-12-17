package metadata

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"sort"
	"sync"

	"github.com/sirupsen/logrus"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Entry is value entry for
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

// EntryList represents a list of entries
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

// EntryMeta is a reduced version of entry for tag map
type EntryMeta struct {
	Address string
	Volume  int64
	Offset  int64
	Size    int64
}

// Layer represents a single layer struct in sorted string table
type Layer struct {
	Name    string
	Volumes []string
	Begin   int
	End     int
}

// Bucket is a struct that ensembles a namespace
type Bucket struct {
	m          sync.RWMutex
	root       string
	Name       string            // bucket name
	MemoMap    map[string]*Entry // key -> entry
	SSTable    []*Layer          // read only layer list
	MemoSize   int64             // size of mempmap
	CreateTime int64             // create timestamp
}

func (b *Bucket) rotate() ([]string, error) {
	entryList := make(EntryList, 0)
	volumeSet := make(map[string]struct{})
	for _, v := range b.MemoMap {
		entryList = append(entryList, v)
		volumeSet[fmt.Sprintf("%v-%v", v.Volume, v.Address)] = struct{}{}
	}
	volumes := make([]string, 0)
	for volume := range volumeSet {
		volumes = append(volumes, volume)
	}
	b.MemoMap = make(map[string]*Entry)
	sort.Sort(entryList)
	err := b.writeLayer(entryList, volumes, len(b.SSTable), len(b.SSTable))
	if err != nil {
		return nil, err
	}
	return volumes, nil
}

func (b *Bucket) deleteLayer(name string) {
	err := os.Remove(path.Join(b.root, name))
	if err != nil {
		logrus.WithError(err).Warn("Delete layer failed")
	}
}

func (b *Bucket) writeLayer(entryList []*Entry, volumes []string, begin int, end int) error {
	bytes, err := json.Marshal(entryList)
	if err != nil {
		logrus.WithError(err).Warn("Marshal JSON failed")
		return status.Error(codes.Internal, "marshal JSON failed")
	}
	name := fmt.Sprintf("%v-%v-%v", b.Name, begin, end)
	file, err := os.Create(path.Join(b.root, name))
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
		Volumes: volumes,
		Begin:   begin,
		End:     end,
	}
	logrus.Debugf("Add new layer %v", name)
	b.SSTable = append(b.SSTable, layer)
	return nil
}

func (b *Bucket) readLayer(name string) ([]*Entry, error) {
	file, err := os.Open(path.Join(b.root, name))
	if err != nil {
		logrus.Warnf("Open file %v failed", name)
		return nil, err
	}
	defer file.Close()
	bytes, err := ioutil.ReadAll(file)
	if err != nil {
		logrus.Warnf("Read file %v failed", name)
		return nil, err
	}
	result := make([]*Entry, 0)
	err = json.Unmarshal(bytes, &result)
	if err != nil {
		logrus.Warnf("Unmarshal JSON from file %v faield", name)
		return nil, err
	}
	return result, nil
}

func (b *Bucket) mergeLayers(layers []*Layer) ([]*Entry, error) {
	logrus.Debugf("Start merging %v layers", len(layers))
	entryMatrix := make([][]*Entry, 0)
	for _, layer := range layers {
		entries, err := b.readLayer(layer.Name)
		if err != nil {
			return nil, err
		}
		entryMatrix = append(entryMatrix, entries)
	}

	return mergeEntryMatrix(entryMatrix), nil
}
