package main

import (
	"sync"
)

type myMap struct {
	mu sync.Mutex
	ma map[int]int
}

func (myMap *myMap) new() {
	myMap.ma = make(map[int]int)
}

func (myMap *myMap) add(key int) {
	myMap.mu.Lock()
	myMap.ma[key]++
	myMap.mu.Unlock()
}

func (myMap *myMap) remove(key int) {
	myMap.mu.Lock()
	myMap.ma[key]--
	if myMap.ma[key] == 0 {
		delete(myMap.ma, key)
	}
	myMap.mu.Unlock()
}

type kvserver struct {
	myMap myMap
}

func main() {

}
