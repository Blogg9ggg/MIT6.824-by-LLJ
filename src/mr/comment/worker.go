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

	// create a workstation folder
	// 创建一个临时文件夹保存这个 worker 的输出
	var err error
	err = os.Mkdir("./data", os.ModePerm)
	position, err = os.MkdirTemp("./data", "*")
	if err != nil {
        log.Fatal(err)
    }
	
	CallAskTask(mapf, reducef)
}

// 
// 处理 map 任务
// 
func makeMap(reply *AskTaskReply, 
	mapf func(string, string) []KeyValue) error {
	// 打开输入的原始文件并将内容读取到 content
	file, err := os.Open(reply.Position[0])
	if err != nil {
		return err
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		return err
	}
	file.Close()

	// 对 content 执行 map 函数得到一个保存 kv 对的列表
	intermediate := []KeyValue{}
	kva := mapf(reply.Position[0], string(content))
	intermediate = append(intermediate, kva...)
	// 根据 key 值排序
	sort.Sort(ByKey(intermediate))
	
	// 利用 ihash 函数将 map 得到的结果分成 NReduce 组 (分给各个 reduce 任务)
	im_groups := make([]ByKey, reply.NReduce)
	for _, kv := range intermediate {
		ihk := ihash(kv.Key) % reply.NReduce
		im_groups[ihk] = append(im_groups[ihk], kv)
	}

	// 将 map 任务得到的结果保存到对应的文件中, 分配到各个 reduce 任务
	// 命名方式: mr-【map 任务 id】-【reduce 任务 id】
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

// 
// 处理 reduce 任务
// 
func makeReduce(reply *AskTaskReply, 
	reducef func(string, []string) string) (pos string, err error) {
	intermediate := []KeyValue{}
	// reply.Position 中记录了保存各个 map 任务结果的文件夹
	// 去各个 map 任务对应的文件夹中取出其结果分割给本 reduce 任务的部分
	// 将所有数据进行 json 解码然后整合在一起重新排序
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

	// 保存输出文件
	// 格式: mr-out-【reduce 任务 id】
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

	// 将所有数据保存完之后再去 rename
	filename = position + "/" + filename
	err = os.Rename(file.Name(), filename)
	if err != nil {
		return "", err
	}

	return filename, nil
}

// 
// worker 端主逻辑
// 
func CallAskTask(mapf func(string, string) []KeyValue, 
	reducef func(string, []string) string) {
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

	args := AskTaskArgs{}
	args.Result = 0
	reply := AskTaskReply{}
	ok := call("Coordinator.DistributeTasks", &args, &reply)
	
	// 循环请求分配任务，每次请求顺便把上次任务的结果发给 coordinator
	// 循环直到全部的 map/reduce 任务结束
	for {
		args = AskTaskArgs{}
		if ok {
			switch reply.Flag {
			case map_task:
				err := makeMap(&reply, mapf)
				if err != nil {	// map error
					args.Result = init_status
				} else {
					args.Result = map_ok
					args.Id = reply.Id
					args.Position = position
				}
			case reduce_task:
				pos, err := makeReduce(&reply, reducef)
				if err != nil {	// reduce error
					args.Result = init_status
				} else {
					args.Result = reduce_ok
					args.Id = reply.Id
					args.Position = pos
				}
			case wait:
				time.Sleep(3*time.Second)
				args.Result = init_status
			case finish:
				log.Fatalf("WORKER(@%v) END.", position)
			}
		} else {
			// ok == false，即与 coordinator 失去连接。
			// 由于本框架不考虑 coordinator 的容灾能力，
			// 故认为这是 coordinator 由于所有任务已完成而主动关机，
			// 故 worker 也结束服务。
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
