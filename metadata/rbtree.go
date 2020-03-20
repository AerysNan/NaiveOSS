package metadata

import "sync"

type RBTreeNode struct {
	key   string
	value interface{}
	l     *RBTreeNode
	r     *RBTreeNode
	red   bool
	size  int
}

type RBTree struct {
	mu   sync.RWMutex
	root *RBTreeNode
}

func newTree() *RBTree {
	return &RBTree{}
}

func newNode(key string, value interface{}, red bool, size int) *RBTreeNode {
	return &RBTreeNode{
		key:   key,
		value: value,
		red:   red,
		size:  size,
	}
}

func (tree *RBTree) get(key string) (interface{}, bool) {
	tree.mu.RLock()
	defer tree.mu.RUnlock()
	return tree.search(tree.root, key)
}

func (tree *RBTree) search(node *RBTreeNode, key string) (interface{}, bool) {
	if node == nil {
		return nil, false
	}
	if node.key == key {
		return node.value, true
	}
	if node.key > key {
		return tree.search(node.l, key)
	}
	return tree.search(node.r, key)
}

func (tree *RBTree) put(key string, value interface{}) {
	tree.mu.Lock()
	defer tree.mu.Unlock()
	tree.root = tree.insert(tree.root, key, value)
	tree.root.red = false
}

func (tree *RBTree) insert(node *RBTreeNode, key string, value interface{}) *RBTreeNode {
	if node == nil {
		return newNode(key, value, true, 1)
	}
	if node.key == key {
		node.value = value
	} else if node.key > key {
		node.l = tree.insert(node.l, key, value)
	} else {
		node.r = tree.insert(node.r, key, value)
	}
	// rotate to keep balance
	if !tree.isRed(node.l) && tree.isRed(node.r) {
		node = tree.rotateL(node)
	}
	if tree.isRed(node.l) && tree.isRed(node.l.l) {
		node = tree.rotateR(node)
	}
	if tree.isRed(node.l) && tree.isRed(node.r) {
		tree.recolor(node)
	}
	node.size = tree.sizeNode(node.l) + tree.sizeNode(node.r) + 1
	return node
}

func (tree *RBTree) isRed(node *RBTreeNode) bool {
	return node != nil && node.red
}

func (tree *RBTree) size() int {
	return tree.sizeNode(tree.root)
}

func (tree *RBTree) sizeNode(node *RBTreeNode) int {
	if node == nil {
		return 0
	}
	return node.size
}

func (tree *RBTree) rotateL(node *RBTreeNode) *RBTreeNode {
	if node == nil || node.r == nil {
		return node
	}
	x := node.r
	node.r = x.l
	x.l = node
	x.red = node.red
	node.red = true
	x.size = node.size
	node.size = tree.sizeNode(node.l) + tree.sizeNode(node.r) + 1
	return x
}

func (tree *RBTree) rotateR(node *RBTreeNode) *RBTreeNode {
	if node == nil || node.l == nil {
		return node
	}
	x := node.l
	node.l = x.r
	x.r = node
	x.red = node.red
	node.red = true
	x.size = node.size
	node.size = tree.sizeNode(node.l) + tree.sizeNode(node.r) + 1
	return x
}

func (tree *RBTree) recolor(node *RBTreeNode) {
	if node == nil || node.l == nil || node.r == nil {
		return
	}
	node.red = !node.red
	node.l.red = !node.l.red
	node.r.red = !node.r.red
}

func (tree *RBTree) isBalanceNode(node *RBTreeNode) (int, bool) {
	if node == nil {
		return -1, true
	}
	heightL, balanceL := tree.isBalanceNode(node.l)
	heightR, balanceR := tree.isBalanceNode(node.r)
	if !balanceL || !balanceR {
		return -1, false
	}
	if heightL > heightR {
		return heightL + 1, true
	}
	return heightR + 1, true
}

func (tree *RBTree) isBalance() bool {
	_, balance := tree.isBalanceNode(tree.root)
	return balance
}
