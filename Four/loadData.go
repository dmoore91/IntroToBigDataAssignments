package main

import (
	"fmt"
	"time"
)

func main() {
	start := time.Now()

	t := time.Now()
	elapsed := t.Sub(start)
	fmt.Println(elapsed)
}
