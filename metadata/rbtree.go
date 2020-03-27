package metadata

import "sync"

type RBTreeNode struct {
	Key string
	// cannot use interface{} here, or it will be unmarshalled into map[string]interface{}
	Value *Entry
	L     *RBTreeNode
	R     *RBTreeNode
	Red   bool
	Size  int
}

type RBTree struct {
	mu   sync.RWMutex
	Root *RBTreeNode
}

func newRBTree() *RBTree {
	return &RBTree{}
}

func newNode(key string, value *Entry, red bool, size int) *RBTreeNode {
	return &RBTreeNode{
		Key:   key,
		Value: value,
		Red:   red,
		Size:  size,
	}
}

func (tree *RBTree) get(key string) (*Entry, bool) {
	tree.mu.RLock()
	defer tree.mu.RUnlock()
	return tree.search(tree.Root, key)
}

func (tree *RBTree) search(node *RBTreeNode, key string) (*Entry, bool) {
	if node == nil {
		return nil, false
	}
	if node.Key == key {
		return node.Value, true
	}
	if node.Key > key {
		return tree.search(node.L, key)
	}
	return tree.search(node.R, key)
}

func (tree *RBTree) put(key string, value *Entry) {
	tree.mu.Lock()
	defer tree.mu.Unlock()
	tree.Root = tree.insert(tree.Root, key, value)
	tree.Root.Red = false
}

func (tree *RBTree) insert(node *RBTreeNode, key string, value *Entry) *RBTreeNode {
	if node == nil {
		return newNode(key, value, true, 1)
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

func (tree *RBTree) isRed(node *RBTreeNode) bool {
	return node != nil && node.Red
}

func (tree *RBTree) size() int {
	return tree.sizeNode(tree.Root)
}

func (tree *RBTree) sizeNode(node *RBTreeNode) int {
	if node == nil {
		return 0
	}
	return node.Size
}

func (tree *RBTree) rotateL(node *RBTreeNode) *RBTreeNode {
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

func (tree *RBTree) rotateR(node *RBTreeNode) *RBTreeNode {
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

func (tree *RBTree) recolor(node *RBTreeNode) {
	if node == nil || node.L == nil || node.R == nil {
		return
	}
	node.Red = !node.Red
	node.L.Red = !node.L.Red
	node.R.Red = !node.R.Red
}

func (tree *RBTree) isBalanceNode(node *RBTreeNode) (int, bool) {
	if node == nil {
		return -1, true
	}
	heightL, balanceL := tree.isBalanceNode(node.L)
	heightR, balanceR := tree.isBalanceNode(node.R)
	if !balanceL || !balanceR {
		return -1, false
	}
	if heightL > heightR {
		return heightL + 1, true
	}
	return heightR + 1, true
}

func (tree *RBTree) isBalance() bool {
	_, balance := tree.isBalanceNode(tree.Root)
	return balance
}

func (tree *RBTree) toList() []*Entry {
	return tree.toListNode(tree.Root)
}

func (tree *RBTree) toListNode(node *RBTreeNode) []*Entry {
	result := make([]*Entry, 0)
	if node == nil {
		return result
	}
	result = append(result, tree.toListNode(node.L)...)
	result = append(result, node.Value)
	result = append(result, tree.toListNode(node.R)...)
	return result
}

func (tree *RBTree) getRange(from string, to string) []*Entry {
	return tree.getRangeNode(tree.Root, from, to)
}

func (tree *RBTree) getRangeNode(node *RBTreeNode, from string, to string) []*Entry {
	if node == nil {
		return nil
	}
	if node.Key > to {
		return tree.getRangeNode(node.L, from, to)
	}
	if node.Key < from {
		return tree.getRangeNode(node.R, from, to)
	}
	return append(append(tree.getRangeNode(node.L, from, to), node.Value), tree.getRangeNode(node.R, from, to)...)
}
