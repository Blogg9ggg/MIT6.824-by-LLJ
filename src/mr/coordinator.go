package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"

import "strconv"
// import "fmt"
import "time"
import "sync"
// import "encoding/json"
// import "sort"

var lock sync.Mutex
type Coordinator struct {
	// Your definitions here.
	MapFinish bool
	ReduceFinish bool
	M int
	R int
	OriginFile []string
	MappedFilePos []string		// the floder of map's result ("[MappedFilePos[i]]/mr-[Id]-*")
	ReducedFilePos []string		// the file of reduce's result (ReducedFilePos[Id] == "???/mr-out-[Id]")
	MapLastTime []int64
	ReduceLastTime []int64
}

// Your code here -- RPC handlers for the worker to call.
const (
	// args.Result
	init_status		int = 0
	map_ok			int = 1
	reduce_ok		int = 2
	// reply.Flag
	map_task 		int = 1
	reduce_task 	int = 2
	wait 			int = 3
	finish 			int = 4
)
func (c *Coordinator) DistributeTasks(args *AskTaskArgs, reply *AskTaskReply) error {
	if c.MapFinish && c.ReduceFinish {
		reply.Flag = finish
		return nil
	}

	switch args.Result {
	case map_ok:
		lock.Lock()
		if c.MappedFilePos[args.Id] == "" {
			c.MappedFilePos[args.Id] = args.Position
		}
		lock.Unlock()
	case reduce_ok:
		lock.Lock()
		if c.ReducedFilePos[args.Id] == "" {
			c.ReducedFilePos[args.Id] = "./mr-out-" + strconv.Itoa(args.Id)		
			err := os.Rename(args.Position, c.ReducedFilePos[args.Id])
			if err != nil {
				log.Fatalf("cannot rename %v", args.Position)
			}
		}
		lock.Unlock()
	}

	nowtime := time.Now().Unix()
	if !c.MapFinish {
		tfinish := true
		for i := 0; i < c.M && !c.MapFinish; i++ {
			if c.MappedFilePos[i] == "" {
				lock.Lock()
				if nowtime - c.MapLastTime[i] > 10 {	
					reply.Flag = map_task
					reply.Position = append(reply.Position, c.OriginFile[i])
					reply.Id = i
					reply.NReduce = c.R
					reply.NMap = c.M

					c.MapLastTime[i] = nowtime
					
					lock.Unlock()
					return nil
				}
				lock.Unlock()
				tfinish = false
			}
		}

		lock.Lock()
		if tfinish && !c.MapFinish {
			c.MapFinish = true
		}
		lock.Unlock()
	}

	// assert: c.MapFinish == true
	tfinish := true
	for i := 0; i < c.R ; i++ {
		lock.Lock()
		if c.ReducedFilePos[i] == "" {
			if nowtime - c.ReduceLastTime[i] > 10 {
				reply.Flag = reduce_task
				
				for j := 0; j < c.M; j++ {
					reply.Position = append(reply.Position, c.MappedFilePos[j])
				}
				reply.Id = i
				reply.NReduce = c.R
				reply.NMap = c.M

				c.ReduceLastTime[i] = nowtime
				lock.Unlock()

				return nil
			}
			tfinish = false
		}
		lock.Unlock()
	}

	reply.Flag = wait	// wait
	lock.Lock()
	if tfinish && !c.ReduceFinish {
		c.ReduceFinish = true		
		if c.MapFinish {
			reply.Flag = finish	
		}
	}
	lock.Unlock()

	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.
	lock.Lock()
	if c.MapFinish && c.ReduceFinish {
		ret = true
	}
	lock.Unlock()

	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	lock.Lock()
	c.MapFinish = false
	c.ReduceFinish = false
	c.M = len(files)
	c.R = nReduce
	c.OriginFile = files
	
	c.MappedFilePos = make([]string, c.M)
	c.MapLastTime = make([]int64, c.M)

	c.ReducedFilePos = make([]string, c.R)
	c.ReduceLastTime = make([]int64, c.R)
	lock.Unlock()
	
	c.server()
	return &c
}
