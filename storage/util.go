package storage

import (
	"math/rand"
	"time"
)

func generateRandomNumber(start int, end int, count int) []int {
	if end < start || (end-start) < count {
		return nil
	}
	nums := make([]int, 0)
	exist := make(map[int]struct{})
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	for len(nums) < count {
		num := r.Intn(end-start) + start
		_, ok := exist[num]
		if !ok {
			nums = append(nums, num)
			exist[num] = struct{}{}
		}
	}
	return nums
}
