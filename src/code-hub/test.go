package main
 
import (
    "fmt"
    // "time"
)
 
func main() {
    test := make(map[int]string)
	test[10] = "aaa"

    for k, v := range test {
        fmt.Printf("%d %s\n", k, v)
        delete(test, k)
    }
}