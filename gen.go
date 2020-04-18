package main

import (
	crand "crypto/rand"
	"encoding/base64"
	"fmt"
	"os"
	"strconv"
)

func randstring(n int) string {
	b := make([]byte, 2*n)
	_, _ = crand.Read(b)
	s := base64.URLEncoding.EncodeToString(b)
	return s[0:n]
}

func main() {
	n, _ := strconv.Atoi(os.Args[1])
	for i := 1; i <= n; i++ {
		value, _ := strconv.Atoi(os.Args[2])
		s := randstring(value)
		file, _ := os.OpenFile(fmt.Sprintf("%v.obj", i), os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0766)
		_, _ = file.Write([]byte(s))
		file.Close()
	}
}
