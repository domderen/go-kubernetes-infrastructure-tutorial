package main

import "fmt"

func main() {
	showAppend("Spare capacity: append can reuse storage", 3)
	showAppend("Full capacity: append needs new storage", 1)
	showNestedMutation()
	showNestedReplacement()
}

func showNestedMutation() {
	original := [][]int{{10}}
	copied := make([][]int, len(original))
	copy(copied, original)
	copied[0][0] = 99

	fmt.Println("Shallow copy: mutate a shared inner element")
	fmt.Printf("  original: %v, copied: %v\n", original, copied)
}

func showNestedReplacement() {
	original := [][]int{{10}}
	copied := make([][]int, len(original))
	copy(copied, original)
	copied[0] = []int{99}

	fmt.Println("Shallow copy: replace an element of the independent outer array")
	fmt.Printf("  original: %v, copied: %v\n", original, copied)
}

func showAppend(label string, capacity int) {
	original := make([]int, 1, capacity)
	original[0] = 10

	extended := append(original, 20)
	extended[0] = 99

	fmt.Println(label)
	fmt.Printf("  original: %v, length=%d, capacity=%d\n", original, len(original), cap(original))
	fmt.Printf("  extended: %v, length=%d, capacity=%d\n", extended, len(extended), cap(extended))
}
