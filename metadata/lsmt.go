package metadata

type LSMT struct {
	Layers [][]*Layer
	Size   int
}

func newLSTM() *LSMT {
	lsmt := new(LSMT)
	lsmt.Layers = make([][]*Layer, 0)
	lsmt.Size = 0
	return lsmt
}

func (lsmt *LSMT) clearLevel(level int) {
	lsmt.Layers[level] = make([]*Layer, 0)
}

func (lsmt *LSMT) pushLayer(layer *Layer, level int) {
	for len(lsmt.Layers) <= level {
		lsmt.Layers = append(lsmt.Layers, make([]*Layer, 0))
	}
	lsmt.Layers[level] = append(lsmt.Layers[level], layer)
	lsmt.Size++
}
