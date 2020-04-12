package metadata

import (
	"crypto/rand"
	"math/big"
)

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

func entrySliceRange(list []*Entry, from string, to string) []*Entry {
	if len(list) == 0 || from > to || list[0].Key > to || list[len(list)-1].Key < from {
		return nil
	}
	var L, R int
	if from < list[0].Key {
		L = 0
	} else {
		l, r := 0, len(list)-1
		for l < r {
			mid := (l + r) / 2
			if list[mid].Key < from {
				l = mid + 1
			} else {
				r = mid
			}
		}
		L = l
	}
	if to > list[len(list)-1].Key {
		R = len(list) - 1
	} else {
		l, r := 0, len(list)-1
		for l < r {
			mid := (l + r + 1) / 2
			if list[mid].Key > to {
				r = mid - 1
			} else {
				l = mid
			}
		}
		R = l
	}
	if L > R || L < 0 || L >= len(list) || R < 0 || R >= len(list) {
		return nil
	}
	return list[L : R+1]
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}
