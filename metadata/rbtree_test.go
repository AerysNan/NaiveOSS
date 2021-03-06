package metadata

import (
	crand "crypto/rand"
	"encoding/base64"
	"fmt"
	"strconv"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
)

func randstring(n int) string {
	b := make([]byte, 2*n)
	_, _ = crand.Read(b)
	s := base64.URLEncoding.EncodeToString(b)
	return s[0:n]
}

func TestRBTreePutThenGet(t *testing.T) {
	t.Log("Running TestRBTreePutThenGet...")
	assert := assert.New(t)
	tree := newRBTree()
	n := 100000
	m := make(map[string]string)
	for i := 0; i < n; i++ {
		k := strconv.Itoa(i)
		v := randstring(20)
		tree.put(k, &Entry{Key: v})
		m[k] = v
	}
	assert.Equal(len(m), tree.size(), "Size of red-black tree index and hash index shouldbe the same.")
	for k, v := range m {
		actual, ok := tree.get(k)
		assert.True(ok, "Red-black tree index shoud return an inserted value.")
		assert.Equal(v, actual.Key, "Red-black tree index and hash index should return the same value.")
	}
	t.Log("Passed.")
}

func TestBRTreeConcurrentPutAndGet(t *testing.T) {
	t.Log("Running TestBRTreeConcurrentPutAndGet...")
	assert := assert.New(t)
	group := 100
	n := 1000
	tree := newRBTree()
	m := sync.Map{}
	waitGroup := sync.WaitGroup{}
	waitGroup.Add(group)
	for i := 0; i < group; i++ {
		go func(group int) {
			defer waitGroup.Done()
			for j := 0; j < n; j++ {
				k := strconv.Itoa(group*n + j)
				v := randstring(20)
				tree.put(k, &Entry{Key: v})
				m.Store(k, v)
			}
		}(i)
	}
	waitGroup.Wait()
	m.Range(func(k interface{}, v interface{}) bool {
		actual, ok := tree.get(k.(string))
		assert.True(ok, "Red-black tree index shoud return an inserted value.")
		assert.Equal(v.(string), actual.Key, "Red-black tree index and hash index should return the same value.")
		return true
	})
	t.Log("Passed.")
}

func TestRBTreeBalance(t *testing.T) {
	t.Log("Running TestBRTreeBalance...")
	assert := assert.New(t)
	tree := newRBTree()
	n := 100000
	for i := 0; i < n; i++ {
		tree.put(strconv.Itoa(i), nil)
	}
	assert.True(tree.isBalance())
	t.Log("Passed.")
}

func TestRBTreeRange(t *testing.T) {
	t.Log("Running TestRBTreeBalance...")
	assert := assert.New(t)
	tree := newRBTree()
	n := 100000
	for i := 0; i < n; i++ {
		tree.put(fmt.Sprintf("%06d", i), nil)
	}
	assert.Equal(10001, len(tree.getRange("010000", "020000")), "Key from 10000 to 20000 should have 10001 entries")
	assert.Equal(10001, len(tree.getRange("#", "010000")), "Key from # to 20000 should have 10001 entries")
	assert.Equal(10000, len(tree.getRange("090000", "?")), "Key from 90000 to ? should have 10000 entries")
}
