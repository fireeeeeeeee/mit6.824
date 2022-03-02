package main

import (
	"fmt"
	"time"
)

func read(ch chan int) {
	for data := range ch {
		fmt.Println(data)
	}
	fmt.Println("break")
}

func write(ch chan int) {

	for i := 0; i < 10; i++ {
		if i%10 == 0 {
			time.Sleep(time.Millisecond * 1000)
		}
		ch <- i
	}
}

func main() {
	ma := make(map[int]int)
	ma[0] = 0
	a, b := ma[0]
	fmt.Println(a, b)

}
