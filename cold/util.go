package cold

import (
	"math/rand"

	"github.com/klauspost/reedsolomon"
	"github.com/sirupsen/logrus"
)

func encoderInit(config *Config) {
	var err error
	enc, err = reedsolomon.New(config.DataShard, config.ParityShard)
	if err != nil {
		logrus.WithError(err).Fatal("Encoder init failed")
	}
}

func generateRandomNumber(start int, end int, count int) []int {
	if end < start || (end-start) < count {
		return nil
	}
	nums := make([]int, end-start)
	for i := start; i < end; i++ {
		nums[i] = i
	}
	rand.Shuffle(len(nums), func(i, j int) { nums[i], nums[j] = nums[j], nums[i] })
	return nums[:count]
}
