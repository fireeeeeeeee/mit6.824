package main

import (
	"fmt"
	"math/rand"
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

func fun() {

	defer fmt.Println(1)
	defer fmt.Println(2)
}

func main() {
	for i := 0; i < 100; i++ {
		ms := 200 + rand.Intn(1+rand.Intn(2000))
		fmt.Println(ms)
	}

}
