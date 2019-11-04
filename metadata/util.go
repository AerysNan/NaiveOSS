package metadata

import (
	"encoding/json"
	"io/ioutil"
	"os"

	"github.com/sirupsen/logrus"
)

func readLayer(name string) ([]*Entry, error) {
	file, err := os.Open(name)
	if err != nil {
		logrus.Warnf("Open file %v failed", name)
		return nil, err
	}
	defer file.Close()
	bytes, err := ioutil.ReadAll(file)
	if err != nil {
		logrus.Warnf("Read file %v failed", name)
		return nil, err
	}
	result := make([]*Entry, 0)
	err = json.Unmarshal(bytes, &result)
	if err != nil {
		logrus.Warnf("Unmarshal JSON from file %v faield", name)
		return nil, err
	}
	return result, nil
}

func mergeLayers(layers []*Layer) ([]*Entry, error) {
	logrus.Debugf("Start merging %v layers", len(layers))
	entryMatrix := make([][]*Entry, 0)
	for _, layer := range layers {
		entries, err := readLayer(layer.Name)
		if err != nil {
			return nil, err
		}
		entryMatrix = append(entryMatrix, entries)
	}

	return mergeEntryMatrix(entryMatrix), nil
}

func mergeEntryMatrix(entryMatrix [][]*Entry) []*Entry {
	result := make([]*Entry, 0)
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
