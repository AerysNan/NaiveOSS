package metadata

import (
	"encoding/json"
	"fmt"
	"os"
	"oss/osserror"
	"sort"

	"github.com/sirupsen/logrus"
)

type Entry struct {
	Key     string
	Tag     string
	Address string
	Volume  int
	Offset  int
	Size    int
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

type Layer struct {
	Name string
	Size int
}

type Bucket struct {
	Name      string            // bucket name
	TagMap    map[string]string // tag -> key
	MemoMap   map[string]*Entry // key -> entry
	SSTable   []*Layer          // read only layer list
	MemoSize  int               // size of mempmap
	MemoCount int               // # of read only layer in memory
}

func (b *Bucket) createNewLayer() error {
	entryList := make(EntryList, 0)
	for _, v := range b.MemoMap {
		entryList = append(entryList, v)
	}
	b.MemoMap = make(map[string]*Entry)
	sort.Sort(entryList)
	bytes, err := json.Marshal(entryList)
	if err != nil {
		logrus.WithError(err).Warn("Marshal JSON failed")
		return osserror.ErrServerInternal
	}
	name := fmt.Sprintf("%v-%v", b.Name, len(b.SSTable))
	file, err := os.Create(name)
	if err != nil {
		logrus.WithField("bucket", b.Name).WithError(err).Warn("Create layer file failed")
		return osserror.ErrServerInternal
	}
	defer file.Close()
	_, err = file.Write(bytes)
	if err != nil {
		logrus.WithField("bucket", b.Name).WithError(err).Warn("Write layer file failed")
		return osserror.ErrServerInternal
	}
	layer := &Layer{
		Name: name,
		Size: len(bytes),
	}
	b.SSTable = append(b.SSTable, layer)
	return nil
}
