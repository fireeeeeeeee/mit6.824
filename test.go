package main

type node struct {
	val int
}

func test(t *node) {
	t.val = 2
}

func main() {
	t := node{}
	test(&t)

}
