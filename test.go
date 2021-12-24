package main

import (
	"container/list"
	"fmt"
	"os"
)

type Node struct {
	val int
}

func Add(l *list.List) {
	l.PushBack(&Node{1})
}

func main() {

	l := list.New()
	Add(l)
	n := l.Front().Value.(*Node)
	n.val++
	n2 := l.Front().Value.(*Node)
	fmt.Println(n2)
	_, err := os.Create("test.go")
	os.Stat()
	fmt.Println(err)
}
