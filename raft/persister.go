package raft

import (
	"io/ioutil"
	"os"
	"path"
	"sync"

	"github.com/sirupsen/logrus"
)

type Persister struct {
	mu   sync.Mutex
	root string
}

func MakePersister(root string) *Persister {
	return &Persister{
		root: root,
	}
}

func (ps *Persister) SaveRaftState(state []byte) {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	file, err := os.OpenFile(path.Join(ps.root, "state"), os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0766)
	if err != nil {
		logrus.WithError(err).Error("Open raft state file failed")
		return
	}
	defer file.Close()
	_, err = file.Write(state)
	if err != nil {
		logrus.WithError(err).Error("Write raft state file failed")
		return
	}
}

func (ps *Persister) ReadRaftState() []byte {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	file, err := os.OpenFile(path.Join(ps.root, "state"), os.O_RDONLY, 0766)
	if err != nil {
		logrus.WithError(err).Error("Open raft state file failed")
		return nil
	}
	defer file.Close()
	bytes, err := ioutil.ReadAll(file)
	if err != nil {
		logrus.WithError(err).Error("Read raft state file failed")
		return nil
	}
	return bytes
}

func (ps *Persister) RaftStateSize() int {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	info, err := os.Stat(path.Join(ps.root, "state"))
	if err != nil {
		logrus.WithError(err).Error("Get raft state file size failed")
		return -1
	}
	return int(info.Size())
}

// Save both Raft state and K/V snapshot as a single atomic action,
// to help avoid them getting out of sync.
func (ps *Persister) SaveStateAndSnapshot(state []byte, snapshot []byte) {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	file, err := os.OpenFile(path.Join(ps.root, "state"), os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0766)
	if err != nil {
		logrus.WithError(err).Error("Open raft state file failed")
		return
	}
	defer file.Close()
	_, err = file.Write(state)
	if err != nil {
		logrus.WithError(err).Error("Write raft state file failed")
		return
	}
	snapshotFile, err := os.OpenFile(path.Join(ps.root, "snapshot"), os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0766)
	if err != nil {
		logrus.WithError(err).Error("Open snapshot file failed")
		return
	}
	defer snapshotFile.Close()
	_, err = snapshotFile.Write(snapshot)
	if err != nil {
		logrus.WithError(err).Error("Write snapshot file failed")
		return
	}
}

func (ps *Persister) ReadSnapshot() []byte {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	file, err := os.OpenFile(path.Join(ps.root, "snapshot"), os.O_RDONLY, 0766)
	if err != nil {
		logrus.WithError(err).Error("Open snapshot file failed")
		return nil
	}
	defer file.Close()
	bytes, err := ioutil.ReadAll(file)
	if err != nil {
		logrus.WithError(err).Error("Read snapshot file failed")
		return nil
	}
	return bytes
}

func (ps *Persister) SnapshotSize() int {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	info, err := os.Stat(path.Join(ps.root, "snapshot"))
	if err != nil {
		logrus.WithError(err).Error("Get snapshot file size failed")
		return -1
	}
	return int(info.Size())
}
