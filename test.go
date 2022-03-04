package main

import "fmt"

func main() {
	a := make(chan int)
	select {
	case a <- 1:
		fmt.Println(1)
	default:
		fmt.Println(2)
	}

}
