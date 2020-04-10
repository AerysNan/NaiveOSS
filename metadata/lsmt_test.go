package metadata

import (
	"sort"
	"testing"
)

type BenchmarkEntry struct {
	Key    string
	Value  string
	Delete bool
}

type BenchmarkLayer []*BenchmarkEntry

func (entries BenchmarkLayer) Less(i, j int) bool {
	return entries[i].Key < entries[j].Key
}

func (entries BenchmarkLayer) Swap(i, j int) {
	entries[i], entries[j] = entries[j], entries[i]
}

func (entries BenchmarkLayer) Len() int {
	return len(entries)
}

type BenchmarkLevel struct {
	Layers    []BenchmarkLayer
	Threshold int
}

func newBenchmarkLevel(threshold int) *BenchmarkLevel {
	return &BenchmarkLevel{
		Layers:    []BenchmarkLayer{make([]*BenchmarkEntry, 0)},
		Threshold: threshold,
	}
}

func (level *BenchmarkLevel) put(key string, value string) {
	layer := level.Layers[len(level.Layers)-1]
	if len(layer) >= level.Threshold {
		level.Layers = append(level.Layers, make([]*BenchmarkEntry, 0))
	}
	level.Layers[len(level.Layers)-1] = append(level.Layers[len(level.Layers)-1], &BenchmarkEntry{
		Key:    key,
		Value:  value,
		Delete: false,
	})
}

func (level *BenchmarkLevel) get(key string) (string, bool) {
	layers := level.Layers
	for i := len(layers) - 1; i >= 0; i-- {
		layer := layers[i]
		l, h := 0, len(layer)-1
		for l <= h {
			m := l + (h-l)/2
			if layer[m].Key == key {
				return layer[m].Value, true
			} else if layer[m].Key > key {
				h = m - 1
			} else {
				l = m + 1
			}
		}
	}
	return "", false
}

func makeBenchmarkLayers(layerCount int, layerSize int) []BenchmarkLayer {
	result := make([]BenchmarkLayer, layerCount)
	for i := 0; i < layerCount; i++ {
		entries := makeBenchmarkEntries(layerSize)
		sort.Sort(entries)
		result[i] = entries
	}
	return result
}

func makeBenchmarkEntries(count int) BenchmarkLayer {
	result := make([]*BenchmarkEntry, count)
	for i := 0; i < count; i++ {
		result[i] = &BenchmarkEntry{
			Key:    randstring(5),
			Value:  randstring(20),
			Delete: false,
		}
	}
	return result
}

func mergeBenchmarkLayers(entryMatrix []BenchmarkLayer) BenchmarkLayer {
	result := make([]*BenchmarkEntry, 0)
	pos := make([]int, len(entryMatrix))
	for {
		k, index := "", -1
		for i, entries := range entryMatrix {
			if pos[i] >= len(entries) {
				continue
			}
			if len(k) == 0 || entries[pos[i]].Key <= k {
				k = entries[pos[i]].Key
				index = i
			}
		}
		if index < 0 {
			break
		}
		if !entryMatrix[index][pos[index]].Delete {
			result = append(result, entryMatrix[index][pos[index]])
		}
		for i, entries := range entryMatrix {
			if pos[i] < len(entries) && entries[pos[i]].Key == k {
				pos[i]++
			}
		}
	}
	return result
}

type BenchmarkRBTreeNode struct {
	Key   string
	Value string
	// cannot use interface{} here, or it will be unmarshalled into map[string]interface{}
	L    *BenchmarkRBTreeNode
	R    *BenchmarkRBTreeNode
	Red  bool
	Size int
}

type BenchmarkRBTree struct {
	Root *BenchmarkRBTreeNode
}

func newBenchmarkRBTree() *BenchmarkRBTree {
	return &BenchmarkRBTree{}
}

func newBenchmarkNode(key string, value string, red bool, size int) *BenchmarkRBTreeNode {
	return &BenchmarkRBTreeNode{
		Key:   key,
		Value: value,
		Red:   red,
		Size:  size,
	}
}

func (tree *BenchmarkRBTree) get(key string) (string, bool) {
	return tree.search(tree.Root, key)
}

func (tree *BenchmarkRBTree) search(node *BenchmarkRBTreeNode, key string) (string, bool) {
	if node == nil {
		return "", false
	}
	if node.Key == key {
		return node.Value, true
	}
	if node.Key > key {
		return tree.search(node.L, key)
	}
	return tree.search(node.R, key)
}

func (tree *BenchmarkRBTree) put(key string, value string) {
	tree.Root = tree.insert(tree.Root, key, value)
	tree.Root.Red = false
}

func (tree *BenchmarkRBTree) insert(node *BenchmarkRBTreeNode, key string, value string) *BenchmarkRBTreeNode {
	if node == nil {
		return newBenchmarkNode(key, value, true, 1)
	}
	if node.Key == key {
		node.Value = value
	} else if node.Key > key {
		node.L = tree.insert(node.L, key, value)
	} else {
		node.R = tree.insert(node.R, key, value)
	}
	// rotate to keep balance
	if !tree.isRed(node.L) && tree.isRed(node.R) {
		node = tree.rotateL(node)
	}
	if tree.isRed(node.L) && tree.isRed(node.L.L) {
		node = tree.rotateR(node)
	}
	if tree.isRed(node.L) && tree.isRed(node.R) {
		tree.recolor(node)
	}
	node.Size = tree.sizeNode(node.L) + tree.sizeNode(node.R) + 1
	return node
}

func (tree *BenchmarkRBTree) isRed(node *BenchmarkRBTreeNode) bool {
	return node != nil && node.Red
}

func (tree *BenchmarkRBTree) size() int {
	return tree.sizeNode(tree.Root)
}

func (tree *BenchmarkRBTree) sizeNode(node *BenchmarkRBTreeNode) int {
	if node == nil {
		return 0
	}
	return node.Size
}

func (tree *BenchmarkRBTree) rotateL(node *BenchmarkRBTreeNode) *BenchmarkRBTreeNode {
	if node == nil || node.R == nil {
		return node
	}
	x := node.R
	node.R = x.L
	x.L = node
	x.Red = node.Red
	node.Red = true
	x.Size = node.Size
	node.Size = tree.sizeNode(node.L) + tree.sizeNode(node.R) + 1
	return x
}

func (tree *BenchmarkRBTree) rotateR(node *BenchmarkRBTreeNode) *BenchmarkRBTreeNode {
	if node == nil || node.L == nil {
		return node
	}
	x := node.L
	node.L = x.R
	x.R = node
	x.Red = node.Red
	node.Red = true
	x.Size = node.Size
	node.Size = tree.sizeNode(node.L) + tree.sizeNode(node.R) + 1
	return x
}

func (tree *BenchmarkRBTree) recolor(node *BenchmarkRBTreeNode) {
	if node == nil || node.L == nil || node.R == nil {
		return
	}
	node.Red = !node.Red
	node.L.Red = !node.L.Red
	node.R.Red = !node.R.Red
}

func BenchmarkCompaction(b *testing.B) {
	n := 1000
	size := 10
	b.ResetTimer()
	for j := 0; j < b.N; j++ {
		b.StopTimer()
		layers := makeBenchmarkLayers(n, size)
		b.StartTimer()
		i := len(layers) - 100
		for ; i > 0; i -= 99 {
			b.StopTimer()
			buffer := layers[i : i+100]
			b.StartTimer()
			layers[i] = mergeBenchmarkLayers(buffer)
		}
		b.StopTimer()
		buffer := layers[:i+100]
		b.StartTimer()
		layers[0] = mergeBenchmarkLayers(buffer)
	}
}

func BenchmarkLeveledCompaction(b *testing.B) {
	n := 10000
	size := 10
	b.ResetTimer()
	for j := 0; j < b.N; j++ {
		b.StopTimer()
		layers := makeBenchmarkLayers(n, size)
		b.StartTimer()
		for len(layers) > 1 {
			count := (len(layers)-1)/10 + 1
			buffer := make([]BenchmarkLayer, count)
			for i := 0; i < count-1; i++ {
				buffer[i] = mergeBenchmarkLayers(layers[i*10 : (i+1)*10])
			}
			buffer[count-1] = mergeBenchmarkLayers(layers[(count-1)*10:])
			layers = buffer
		}
	}
}

func BenchmarkRBTreePut(b *testing.B) {
	n := 1000000
	b.ResetTimer()
	for j := 0; j < b.N; j++ {
		tree := newBenchmarkRBTree()
		for i := 0; i < n; i++ {
			b.StopTimer()
			key := randstring(5)
			value := randstring(20)
			b.StartTimer()
			tree.put(key, value)
		}
	}
}

func BenchmarkLSMTPut(b *testing.B) {
	n := 1000000
	b.ResetTimer()
	for j := 0; j < b.N; j++ {
		level := newBenchmarkLevel(100)
		for i := 0; i < n; i++ {
			b.StopTimer()
			key := randstring(5)
			value := randstring(20)
			b.StartTimer()
			level.put(key, value)
		}
	}
}

func BenchmarkRBTreeRead(b *testing.B) {
	n := 1000000
	keys := make([]string, n)
	tree := newBenchmarkRBTree()
	for i := 0; i < n; i++ {
		keys[i] = randstring(5)
		value := randstring(20)
		tree.put(keys[i], value)
	}
	b.ResetTimer()
	for j := 0; j < b.N; j++ {
		for i := 0; i < n; i++ {
			tree.get(keys[i])
		}
	}
}

func BenchmarkLSMTRead(b *testing.B) {
	n := 1000000
	keys := make([]string, n)
	level := newBenchmarkLevel(1000)
	for i := 0; i < n; i++ {
		keys[i] = randstring(5)
		value := randstring(20)
		level.put(keys[i], value)
	}
	sort.Sort(level.Layers[len(level.Layers)-1])
	b.ResetTimer()
	for j := 0; j < b.N; j++ {
		for i := 0; i < n; i++ {
			level.get(keys[i])
		}
	}
}
