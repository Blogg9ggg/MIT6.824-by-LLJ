package mr

import "fmt"
import "log"
import "net/rpc"
import "hash/fnv"

import "os"
import "io/ioutil"
import "encoding/json"
import "sort"
import "strconv"
// import "strings"
import "time"


//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// like "./data/???"
var position string

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// create a workstation folder
	var err error
	err = os.Mkdir("./data", os.ModePerm)

	position, err = os.MkdirTemp("./data", "*")
	if err != nil {
        log.Fatal(err)
    }
	
	CallAskTask(mapf, reducef)
}

func makeMap(reply *AskTaskReply, 
	mapf func(string, string) []KeyValue) error {

	file, err := os.Open(reply.Position[0])
	if err != nil {
		return err
	}
	// origin file's content
	content, err := ioutil.ReadAll(file)
	if err != nil {
		return err
	}
	file.Close()

	intermediate := []KeyValue{}
	kva := mapf(reply.Position[0], string(content))
	intermediate = append(intermediate, kva...)
	sort.Sort(ByKey(intermediate))
	
	im_groups := make([]ByKey, reply.NReduce)
	for _, kv := range intermediate {
		ihk := ihash(kv.Key) % reply.NReduce
		im_groups[ihk] = append(im_groups[ihk], kv)
	}

	for i := 0; i < reply.NReduce; i++ {
		filename := "mr-" + strconv.Itoa(reply.Id) + "-" + strconv.Itoa(i)
		file, err = ioutil.TempFile(position, filename)
		if err != nil {
			return  err
		}

		enc := json.NewEncoder(file)
		for _, kv := range im_groups[i] {
			err = enc.Encode(&kv)
			if err != nil {
				return err
			}
		}
		err := os.Rename(file.Name(), position + "/" + filename)
		if err != nil {
			return err
		}
		file.Close()
	}

	return nil
}

func makeReduce(reply *AskTaskReply, 
	reducef func(string, []string) string) (pos string, err error) {

	intermediate := []KeyValue{}
	for i := 0; i < reply.NMap; i++ {
		filename := reply.Position[i] + "/mr-" + strconv.Itoa(i) + "-" + strconv.Itoa(reply.Id)
		file, err := os.Open(filename)
		if err != nil {
			return "", err
		}

		dec := json.NewDecoder(file)
		for {
		  	var kv KeyValue
		  	if err := dec.Decode(&kv); err != nil {
				break
		  	}
		  	intermediate = append(intermediate, kv)
		}
		file.Close()
	}
	
	sort.Sort(ByKey(intermediate))

	filename := "mr-out-" + strconv.Itoa(reply.Id)
	file, err := ioutil.TempFile(position, filename)
	if err != nil {
		return "", err
	}
	defer file.Close()
	
	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)
		fmt.Fprintf(file, "%v %v\n", intermediate[i].Key, output)
		
		i = j
	}

	filename = position + "/" + filename
	err = os.Rename(file.Name(), filename)
	if err != nil {
		return "", err
	}

	return filename, nil
}

func CallAskTask(mapf func(string, string) []KeyValue, 
	reducef func(string, []string) string) {

	args := AskTaskArgs{}
	args.Result = 0
	reply := AskTaskReply{}
	ok := call("Coordinator.DistributeTasks", &args, &reply)
	
	for {
		args = AskTaskArgs{}
		if ok {
			switch reply.Flag {
			case 1:
				err := makeMap(&reply, mapf)
				if err != nil {	// map error
					args.Result = 0
				} else {
					args.Result = 1
					args.Id = reply.Id
					args.Position = position
				}
			case 2:
				pos, err := makeReduce(&reply, reducef)
				if err != nil {	// reduce error
					args.Result = 0
				} else {
					args.Result = 2
					args.Id = reply.Id
					args.Position = pos
				}
			case 3:
				time.Sleep(3*time.Second)
				args.Result = 0
			case 4:
				log.Fatalf("WORKER(@%v) END.", position)
			}
		} else {
			log.Fatalf("WORKER(@%v) END.", position)
		}

		reply = AskTaskReply{}
		ok = call("Coordinator.DistributeTasks", &args, &reply)
	}
	
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
