package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

// Add your RPC definitions here.

type AskTaskArgs struct {
	Result int 	// 0, init; 1, map_ok; 2, reduce_ok
	Id int		// Result = 1, map task ID
				// Result = 2, reduce task ID
	Position string	// Result = 1, the floder of map's result ("[Position]/mr-[Id]-*")
					// Result = 2, the file of reduce's result (Position == "???/mr-out-[Id]")
}

type AskTaskReply struct {
	Flag int			// 0, undefined; 1, map; 2, reduce; 3, wait; 4, finish
	Position []string 	// Flag = 1, Position[0] := a origin input file's positon
						// Flag = 2, Position	:= all mapped result ("???/mr-[0-(NMap-1)]-[0-(NReduce-1)]")
	Id int				// Flag = 1, map task ID. Expected generation: "???/mr-[Id]-*"
						// Flag = 2, reduce task Id. Expected generation: "???/mr-out-[Id]"
	NReduce int
	NMap int
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}