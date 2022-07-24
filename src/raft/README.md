### 基本思路

Raft 的 server 有 3 个角色：leader, candidate, follower。主要有 3 个 rpc：拉票，复制日志和快照，最核心的是前面 2 个。

Raft 的 server 一开始的角色都是 follower，follower 有一个超时时间，如果在这段时间里面没有收到 leader 的消息，

### 知识

* 在 linux 中，`write(fd,...)` 函数并没有直接将数据同步到磁盘中，要达到这个目的可以调用`fsync(fd)` 函数。

### 结果

```
Test (2A): initial election ...
  ... Passed --   3.1  3   59   16106    0
Test (2A): election after network failure ...
  ... Passed --   4.5  3  155   28011    0
Test (2A): multiple elections ...
  ... Passed --   5.4  7  767  133311    0
Test (2B): basic agreement ...
  ... Passed --   0.9  3   16    4320    3
Test (2B): RPC byte count ...
  ... Passed --   2.5  3   47  113548   11
Test (2B): agreement after follower reconnects ...
  ... Passed --   6.0  3  145   36702    8
Test (2B): no agreement if too many followers disconnect ...
  ... Passed --   3.6  5  264   48573    3
Test (2B): concurrent Start()s ...
  ... Passed --   0.7  3   12    3236    6
Test (2B): rejoin of partitioned leader ...
  ... Passed --   6.1  3  206   48674    4
Test (2B): leader backs up quickly over incorrect follower logs ...
  ... Passed --  26.9  5 5161 4229011  102
Test (2B): RPC counts aren't too high ...
  ... Passed --   2.4  3   47   13426   12
Test (2C): basic persistence ...
  ... Passed --   5.8  3  122   31986    7
Test (2C): more persistence ...
  ... Passed --  18.0  5 1287  244766   16
Test (2C): partitioned leader and one follower crash, leader restarts ...
  ... Passed --   2.1  3   41    9575    4
Test (2C): Figure 8 ...
  ... Passed --  27.6  5  956  188311    8
Test (2C): unreliable agreement ...
  ... Passed --   5.4  5  268   87648  246
Test (2C): Figure 8 (unreliable) ...
  ... Passed --  34.5  5 7867 18847875  414
Test (2C): churn ...
  ... Passed --  16.2  5  940  630034  242
Test (2C): unreliable churn ...
  ... Passed --  16.2  5 1038  439758  341
Test (2D): snapshots basic ...
  ... Passed --   6.9  3  136   48034  234
Test (2D): install snapshots (disconnect) ...
  ... Passed --  80.3  3 1886  763573  349
Test (2D): install snapshots (disconnect+unreliable) ...
  ... Passed --  86.8  3 2384  848912  312
Test (2D): install snapshots (crash) ...
  ... Passed --  36.6  3  852  546126  307
Test (2D): install snapshots (unreliable+crash) ...
  ... Passed --  68.0  3 1863  902888  348
Test (2D): crash and restart all servers ...
  ... Passed --  31.0  3  684  183902   60
PASS
ok      6.824/raft      497.491s
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