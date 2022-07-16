# README

跟着 MIT6.824 的 lab1 实现的一个简单的 map/reduce 框架。



# MapReduce

## 核心思想

提供一个分布式计算框架，程序员无需任何分布式的知识就可以在这个框架上编写自己的程序。

经过谷歌大量重复地编写数据处理类的程序，发现所有数据处理的程序都有类似的过程：将一组输入的数据应用 `map` 函数返回一个 k/v 对的结构作为中间数据集，并将具有相同 key 的数据输入到一个 `reduce` 函数中计算，最终返回处理后的结果。

这一计算模型的优势在于非常利于并行化，`map` 的过程可以在多台机器上进行而机器之间不需要相互协调；`reduce` 的执行同样不需要协调，没有相互依赖，可并行执行。 所有的依赖性和协调过程都被隐藏在 `map` 与 `reduce` 函数的数据分发之间。因此这里有一个很重要的细节就是：**map和reduce都是可以各自并行执行的，但reduce执行的前提条件是所有的map函数执行完毕**。



## MIT6.824 Lab1

这个实验给了一些文本（`src/main` 中的 `pg-*.txt`），利用 map/reduce 框架在这些文本上做一些计算。具体做什么计算取决于 worker 端所使用的插件，比如 `wc.so` 插件就是用来实现词频计算，即统计 `pg-*.txt` 这些文本中各个单词的出现次数。

### 编译与运行

插件的代码都在`src/mrapps` 文件夹中，所以若要在 `src/main` 中 build 插件相应的 so 文件：

```
go build -buildmode=plugin ../mrapps/【插件名】.go
```

worker 端运行：

```
go run mrworker.go 【插件名】.so
```

coordinator 端运行：

```
go run mrcoordinator.go pg-*.txt
```

已通过所有的 test：

```
blog@ubuntu:~/6.824/src/main$ ./test-mr.sh 
*** Starting wc test.
2022/07/16 01:27:30 WORKER(@./data/3431031197) END.
2022/07/16 01:27:30 WORKER(@./data/2662446498) END.
--- wc test: PASS
2022/07/16 01:27:33 dialing:dial unix /var/tmp/824-mr-1000: connect: connection refused
*** Starting indexer test.
2022/07/16 01:27:48 WORKER(@./data/1686411937) END.
--- indexer test: PASS
2022/07/16 01:27:51 dialing:dial unix /var/tmp/824-mr-1000: connect: connection refused
*** Starting map parallelism test.
2022/07/16 01:28:08 WORKER(@./data/2560210306) END.
--- map parallelism test: PASS
2022/07/16 01:28:11 dialing:dial unix /var/tmp/824-mr-1000: connect: connection refused
*** Starting reduce parallelism test.
2022/07/16 01:28:29 WORKER(@./data/3029268925) END.
2022/07/16 01:28:32 dialing:dial unix /var/tmp/824-mr-1000: connect: connection refused
--- reduce parallelism test: PASS
*** Starting job count test.
2022/07/16 01:28:59 WORKER(@./data/502440046) END.
2022/07/16 01:28:59 WORKER(@./data/1923260606) END.
2022/07/16 01:28:59 WORKER(@./data/3493630774) END.
--- job count test: PASS
2022/07/16 01:29:02 dialing:dial unix /var/tmp/824-mr-1000: connect: connection refused
*** Starting early exit test.
2022/07/16 01:29:18 WORKER(@./data/3872957942) END.
2022/07/16 01:29:18 WORKER(@./data/3486697207) END.
2022/07/16 01:29:21 dialing:dial unix /var/tmp/824-mr-1000: connect: connection refused
--- early exit test: PASS
*** Starting crash test.
2022/07/16 01:30:00 WORKER(@./data/1500141597) END.
2022/07/16 01:30:01 WORKER(@./data/1737197055) END.
2022/07/16 01:30:01 WORKER(@./data/1675976439) END.
2022/07/16 01:30:01 WORKER(@./data/2197644462) END.
--- crash test: PASS
*** PASSED ALL TESTS
```



### worker 端和 coordinator 端交互逻辑

worker 端不维护信息。coordinator 维护的任务信息如下：

```go
type Coordinator struct {
	// 所有的 map 流程是否已结束
	MapFinish bool
	// 所有的 reduce 流程是否已结束
	ReduceFinish bool
	// 有多少个 map 任务需要完成，在这个 lab 中，输入了多少个文本文件，M 就是多少
	M int
	// 分成多少个可以并行执行的 reduce 任务
	R int
	// 保存原始输入文件的路径
	OriginFile []string

	// 记录保存 map/reduce 任务结果的文件
	MappedFilePos []string		// the floder of map's result ("[MappedFilePos[i]]/mr-[Id]-*")
	ReducedFilePos []string		// the file of reduce's result (ReducedFilePos[Id] == "???/mr-out-[Id]")
	// 记录上次分配 map/reduce 任务的时间
    // 为了不要太浪费计算资源，coordinator 端为每个已分配的任务设置一个时限，超时后才会将已分配的任务发给别的 worker
	MapLastTime []int64
	ReduceLastTime []int64
}
```

两方通过 `DistributeTasks` 这个 rpc 进行交互，定义如下：

```go
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
```

具体流程如下：

1. 开启多个 worker 和 1 个 coordinator。worker 和 coordinator 交互的基本模式是 coordinator 监听并处理 worker 发来的 rpc，给 worker 分配任务并更新任务信息，记录 worker 的计算结果；worker 通过 `DistributeTasks` 这个 rpc 向 coordinator 请求任务，得到任务后执行具体的计算，并将计算结果返回给 coordinator。

2. worker 准备就绪后调用 `DistributeTasks` 向 coordinator 请求任务，顺便发送上一次任务的计算结果。若此次调用是 worker 第一次请求任务或这个 worker 在上一次请求时被 coordinator 让其进入 wait 状态，则此次调用的 `AskTaskArgs` 参数中是没有上次计算的结果的，则 `Result = 0`；若上一次是 map 任务，则 `Result = 1`；若上一次是 reduce 任务，则 `Result = 2`。 `AskTaskArgs` 结构体中的 `Id,Position` 字段根据 `Result` 字段的不同携带不同的信息。

3. coordinator 收到 `DistributeTasks` 调用后，更新任务的完成信息，必要时将计算结果保存起来。若全部计算已经结束，则返回 `Flag = 4` 通知 worker 流程结束。否则遍历各个 map 或 reduce 任务的状态信息，若有未分配或者已分配但是超时的任务就将任务发给 worker，若没有则发送 `Flag = 3` 通知 worker 暂时没有计算任务，建议挂起等待。

   

### tips

* 如何实现文件的原子写入？

  linux 中的 rename 函数是原子性的，故可以利用这个性质，先在一个文件中完成全部数据的写入，然后再去 rename 这个文件。
