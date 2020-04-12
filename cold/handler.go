package cold

import (
	"context"
	"fmt"
	"os"
	pc "oss/proto/cold"
	"path"

	"github.com/klauspost/reedsolomon"
	"github.com/sirupsen/logrus"
)

var enc reedsolomon.Encoder

type Config struct {
	DataShard   int
	ParityShard int
	BSDNum      int
}

type Server struct {
	config  *Config
	Root    string
	BSDs    map[int64]*BSD
	Volumes map[int64]*Volume
}

func NewCold(root string, config *Config) *Server {
	encoderInit(config)
	s := &Server{
		config:  config,
		Root:    root,
		BSDs:    make(map[int64]*BSD),
		Volumes: make(map[int64]*Volume),
	}
	s.bsdInit()
	return s
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

func (s *Server) DeleteVolume(ctx context.Context, request *pc.DeleteVolumeRequest) (*pc.DeleteVolumeResponse, error) {
	volume, ok := s.Volumes[request.VolumeId]
	if ok {
		for _, block := range volume.Blocks {
			err := os.Remove(block.Path)
			if err != nil {
				logrus.WithError(err).Warnf("Delete block %s failed", block.Path)
			}
		}
	}
	return &pc.DeleteVolumeResponse{}, nil
}

func (s *Server) PushCold(ctx context.Context, request *pc.PushColdRequest) (*pc.PushColdResponse, error) {

}

func (s *Server) Read(ctx context.Context, request *pc.ReadRequest) (*pc.ReadResponse, error) {
}

func (s *Server) chooseValidBSDs() []*BSD {
	nums := generateRandomNumber(0, s.config.BSDNum, s.config.DataShard+s.config.ParityShard)
	res := make([]*BSD, 0)
	for _, num := range nums {
		res = append(res, s.BSDs[int64(num)])
	}
	return res
}
