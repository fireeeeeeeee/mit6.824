package main

import (
	"fmt"
	"sync"
)

var a string
var once sync.Once

func setup() {
	a = "hello, world"
}

func doprint() {
	once.Do(setup)
	fmt.Println(a)
}

func twoprint() {
	go doprint()
	go doprint()

}

var ch chan int

func insert() {
	for i := 0; i < 10; i++ {
		ch <- i
	}
	fmt.Println("insert finish")
}

func output() {
	for i := 0; i < 10; i++ {
		fmt.Println(<-ch)

	}
}

type A struct {
	a []int
}

func main() {
	s := A{a: []int{5, 6}}
	fmt.Println(s.a[-1])
}
