package storage

import (
	crand "crypto/rand"
	"encoding/base64"
	"os"
	"testing"
)

const threshold = 1 << 20

type v struct {
	path string
	size int
}

type controller struct {
	size  int
	vs    []*v
	index int
}

func (c *controller) write(data []byte) {
	if c.vs[c.index].size >= threshold {
		c.vs = append(c.vs, &v{
			path: randstring(20),
			size: 0,
		})
		c.index++
	}
	file, _ := os.OpenFile(c.vs[c.index].path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0766)
	_, _ = file.Write(data)
	c.vs[c.index].size += len(data)
	c.size += len(data)
	file.Close()
}

func TestWrite(t *testing.T) {
	n := 1024
	c := &controller{
		size:  0,
		vs:    nil,
		index: 0,
	}
	c.vs = append(c.vs, &v{
		path: randstring(20),
		size: 0,
	})
	for i := 0; i < n; i++ {
		c.write(randbytes(1 << 10))
	}
	for _, v := range c.vs {
		info, _ := os.Stat(v.path)
		t.Logf("%v size: %v\n", v.path, info.Size())
		os.Remove(v.path)
	}
}

func randstring(n int) string {
	b := make([]byte, 2*n)
	_, _ = crand.Read(b)
	s := base64.URLEncoding.EncodeToString(b)
	return s[0:n]
}

func randbytes(n int) []byte {
	b := make([]byte, 2*n)
	_, _ = crand.Read(b)
	return b
}
