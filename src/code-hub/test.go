package main
 
import (
    "fmt"
    "time"
)
 
func main() {
    timer := time.NewTimer(3*time.Second) 
    fmt.Println("timertype: ", timer)
    fmt.Println(time.Now()) 
 
	// time.Sleep(4*time.Second)
	// timer.Stop()
	// timer.Reset(time.Millisecond*10000)
    // if timer.Stop() {
	// 	fmt.Println("YES")
	// }

	// timer.Stop()
	// if !timer.Stop() {
	// 	fmt.Println("c")
	// 	fmt.Println(len(timer.C))
	// }
	time.Sleep(4*time.Second)
	timer.Reset(10*time.Second)
	fmt.Println(len(timer.C))
}