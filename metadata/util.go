package metadata

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
