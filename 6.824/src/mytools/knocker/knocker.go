package knocker

import (
	"fmt"
	"time"
)

type Knocker struct {
	close bool
	ch    chan []interface{}
	cnt   int
}

func (k *Knocker) knock() {
	first := false
	var pre []interface{}
	for !k.close {
		var ok bool
		var data []interface{}
		select {
		case data = <-k.ch:
			pre = data
			ok = true
		default:
			ok = false
			time.Sleep(10 * time.Millisecond)
		}

		if ok {
			first = true
			pre = data
		}

		if first {
			k.cnt++
			fmt.Println(pre, k.cnt)
		}
	}
}

func New() *Knocker {
	rec := &Knocker{false, make(chan []interface{}), 0}
	go rec.knock()
	return rec
}

func (k *Knocker) Add(s ...interface{}) {
	if !k.close {
		k.ch <- s
	}
}

func (k *Knocker) Close() {
	k.close = true
}
