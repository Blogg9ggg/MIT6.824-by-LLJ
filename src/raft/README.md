### 基本思路

Raft 的 server 有 3 个角色：leader, candidate, follower。主要有 3 个 rpc：拉票，复制日志和快照，最核心的是前面 2 个。

Raft 的 server 一开始的角色都是 follower，follower 有一个超时时间，如果在这段时间里面没有收到 leader 的消息，

### 知识

* 在 linux 中，`write(fd,...)` 函数并没有直接将数据同步到磁盘中，要达到这个目的可以调用`fsync(fd)` 函数。

### 结果

```
blog@ubuntu:~/6.824/src/raft$ go test
Test (2A): initial election ...
  ... Passed --   3.1  3   60   16386    0
Test (2A): election after network failure ...
  ... Passed --   4.5  3  152   27679    0
Test (2A): multiple elections ...
  ... Passed --   5.5  7  648  114528    0
Test (2B): basic agreement ...
  ... Passed --   0.9  3   15    4190    3
Test (2B): RPC byte count ...
  ... Passed --   2.4  3   47  113642   11
Test (2B): agreement after follower reconnects ...
  ... Passed --   6.1  3  144   36255    8
Test (2B): no agreement if too many followers disconnect ...
  ... Passed --   3.6  5  257   46978    3
Test (2B): concurrent Start()s ...
  ... Passed --   0.7  3   13    3652    6
Test (2B): rejoin of partitioned leader ...
  ... Passed --   6.1  3  213   50049    4
Test (2B): leader backs up quickly over incorrect follower logs ...
  ... Passed --  26.8  5 5257 4243557  102
Test (2B): RPC counts aren't too high ...
  ... Passed --   2.3  3   45   12872   12
Test (2C): basic persistence ...
  ... Passed --   3.9  3   82   21363    6
Test (2C): more persistence ...
  ... Passed --  18.5  5 1346  255978   16
Test (2C): partitioned leader and one follower crash, leader restarts ...
  ... Passed --   1.9  3   37    8972    4
Test (2C): Figure 8 ...
  ... Passed --  34.7  5 1127  217400    7
Test (2C): unreliable agreement ...
  ... Passed --   5.3  5  263   86742  246
Test (2C): Figure 8 (unreliable) ...
  ... Passed --  35.4  5 9393 29361849  582
Test (2C): churn ...
  ... Passed --  16.5  5 1294 2234277  349
Test (2C): unreliable churn ...
  ... Passed --  16.4  5 1827 1219591  206
Test (2D): snapshots basic ...
  ... Passed --   7.0  3  137   48820  243
Test (2D): install snapshots (disconnect) ...
  ... Passed --  76.9  3 1792  699783  308
Test (2D): install snapshots (disconnect+unreliable) ...
  ... Passed --  86.5  3 2437 1002784  358
Test (2D): install snapshots (crash) ...
  ... Passed --  36.7  3  925  565850  360
Test (2D): install snapshots (unreliable+crash) ...
  ... Passed --  65.0  3 1628  940734  355
Test (2D): crash and restart all servers ...
  ... Passed --  33.4  3  705  188765   67
PASS
ok      6.824/raft      499.982s
```



### tips

* 加锁策略：

  1. 任何需要“等待”的地方都别加锁（如 rpc 调用需要等待响应，channel 输入需要等待输出接收，定时器等待）

* 定时器：使用 `time.Timer`。初始化：`time.NewTimer()`；重新定时：`timer.Reset()`；若定时器超时，会往定时器的内置管道 `timer.C` 中输入消息（在此处其实是扮演一个消息队列的角色）。清空定时器时要注意取出 `timer.C` 中的消息：

  ```go
  func (rf *Raft) clearTimerC() {
  	if !rf.timer.Stop() && len(rf.timer.C)>0 {
  		<- rf.timer.C
  	}
  }
  ```

* persist 持久化可以直到本机与外界通信前再去完成。即要发送或响应一个 RPC 请求之前去做一次持久化即可。

* 待续...

### idea
* 如果 leader 发送心跳时没有收到超过半数的响应，是不是退成 follower 比较好呢?