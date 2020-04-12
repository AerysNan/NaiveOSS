package storage

import (
	"sync"
	"time"
)

type Blob struct {
	Id   string
	Tag  string
	Time time.Time
	m    sync.RWMutex
}

func NewBlob(id, tag string) *Blob {
	return &Blob{
		Id:   id,
		Tag:  tag,
		Time: time.Now(),
		m:    sync.RWMutex{},
	}
}

// Volume represents a virtual data container which serves the metadata server
type Volume struct {
	m       sync.RWMutex
	ID      int64
	Size    int64
	Content int64
	Cold    bool
}

// NewVolume returns a new volume struct
func NewVolume(id int64, config *Config) *Volume {
	return &Volume{
		m:       sync.RWMutex{},
		ID:      id,
		Size:    0,
		Content: 0,
		Cold:    false,
	}
}
