package storage

import (
	"crypto/sha256"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"sync"

	"github.com/sirupsen/logrus"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// BSD stands for block storage device
type BSD struct {
	Address string
}

// NewBSD returns a new block storage device struct
func NewBSD(address string) *BSD {
	return &BSD{
		Address: address,
	}
}

// Block represents a data block stored on disk
type Block struct {
	Tag  string
	Path string
}

// NewBlock returns a new block struct
func NewBlock(tag string, path string) *Block {
	return &Block{
		Tag:  tag,
		Path: path,
	}
}

// Volume represents a virtual data container which serves the metadata server
type Volume struct {
	m         sync.RWMutex
	ID        int64
	Size      int64
	Content   int64
	BlockSize int64
	Blocks    []*Block
}

// NewVolume returns a new volume struct
func NewVolume(id int64, config *Config) *Volume {
	return &Volume{
		m:         sync.RWMutex{},
		ID:        id,
		Size:      0,
		Content:   0,
		BlockSize: -1,
		Blocks:    make([]*Block, 0, config.DataShard+config.ParityShard),
	}
}

func (v *Volume) readFromBlocks(offset, length int64, config *Config) ([]byte, error) {
	sequence := offset / v.BlockSize
	offset = offset % v.BlockSize
	var data []byte
	for {
		v.m.RLock()
		block := v.Blocks[sequence]
		content, err := ioutil.ReadFile(block.Path)
		v.m.RUnlock()
		if err != nil || fmt.Sprintf("%x", sha256.Sum256(content)) != block.Tag {
			err = v.reconstruct(config)
			if err != nil {
				return nil, err
			}
		}
		v.m.RLock()
		file, err := os.Open(block.Path)
		if err != nil {
			v.m.RUnlock()
			logrus.WithError(err).Errorf("Open file %v failed", block.Path)
			return nil, status.Error(codes.Internal, "open data failed")
		}
		bytes := make([]byte, length)
		n, err := file.ReadAt(bytes, offset)
		file.Close()
		v.m.RUnlock()
		if err != nil && n == 0 {
			logrus.WithError(err).Errorf("Read file %v failed", block.Path)
			return nil, status.Error(codes.Internal, "read data failed")
		}
		length -= int64(n)
		data = append(data, bytes[:n]...)
		if length > 0 {
			sequence++
			offset = 0
			continue
		} else {
			break
		}
	}
	return data, nil
}

func (v *Volume) encode(name string, BSDs []*BSD) {
	v.m.RLock()
	f, err := ioutil.ReadFile(name)
	v.m.RUnlock()
	if err != nil {
		logrus.WithError(err).Errorf("Open file %v failed while encoding", name)
		return
	}
	blocks, err := enc.Split(f)
	if err != nil {
		logrus.WithError(err).Errorf("Split file %v failed while encoding", name)
		return
	}
	err = enc.Encode(blocks)
	if err != nil {
		logrus.WithError(err).Errorf("Encode file %v failed while encoding", name)
		return
	}
	for i, block := range blocks {
		path := path.Join(BSDs[i].Address, fmt.Sprintf("%d_%d.dat", v.ID, i))
		tag := fmt.Sprintf("%x", sha256.Sum256(blocks[i]))
		v.Blocks = append(v.Blocks, NewBlock(tag, path))
		err := ioutil.WriteFile(path, block, os.ModePerm)
		if err != nil {
			logrus.WithError(err).Errorf("Write file %s failed", path)
		}
	}
	v.m.Lock()
	defer v.m.Unlock()
	v.BlockSize = int64(len(blocks[0]))
	_ = os.Remove(name)
}

func (v *Volume) reconstruct(config *Config) error {
	v.m.RLock()
	needRepair := make([]int, 0)
	data := make([][]byte, config.DataShard+config.ParityShard)
	for i, block := range v.Blocks {
		f, err := ioutil.ReadFile(block.Path)
		if err != nil || fmt.Sprintf("%x", sha256.Sum256(f)) != block.Tag {
			logrus.WithField("VolumeID", v.ID).Warnf("%s need reconstructing", block.Path)
			data[i] = nil
			needRepair = append(needRepair, i)
		} else {
			data[i] = f
		}
	}
	v.m.RUnlock()
	if len(needRepair) == 0 {
		return nil
	}
	logrus.WithField("VolumeID", v.ID).Warn("Start reconstructing data")
	err := enc.Reconstruct(data)
	if err != nil {
		logrus.WithField("VolumeID", v.ID).Error("Data corrupted")
		return status.Error(codes.Internal, "Data Corrupted")
	}
	ok, err := enc.Verify(data)
	if err != nil || !ok {
		logrus.WithField("VolumeID", v.ID).Error("Data corrupted")
		return status.Error(codes.Internal, "Data Corrupted")
	}
	logrus.WithField("VolumeID", v.ID).Info("Reconstructing data succeed")
	v.m.Lock()
	defer v.m.Unlock()
	for _, n := range needRepair {
		err := ioutil.WriteFile(v.Blocks[n].Path, data[n], os.ModePerm)
		if err != nil {
			logrus.WithError(err).Errorf("Write file %s failed after reconstructing", v.Blocks[n].Path)
		}
	}
	return nil
}
