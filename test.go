package main

import (
	"fmt"
	"sync"
	"time"
)

func compute(wg *sync.WaitGroup) float64 {
	defer wg.Done()
	x := 0.0
	for i := 1; i < 1000000000; i++ {
		x += 1 / float64(i) / float64(i)
	}
	return x
}

func GetTime() float64 {
	return float64(time.Now().UnixNano()) / 1e9
}

func main() {
	wg := sync.WaitGroup{}
	for i := 1; i <= 20; i++ {
		beginTime := GetTime()
		for j := 0; j < i; j++ {
			wg.Add(1)
			go compute(&wg)
		}
		wg.Wait()
		fmt.Println(i, GetTime()-beginTime)
	}

}
