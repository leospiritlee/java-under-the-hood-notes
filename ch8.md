# 第8章 线程池：ctl 这一个 int 装了什么

**一句话**：把状态和人数压进同一个 int，是为了让"还能不能收活"和"现在几个人"永远读到同一时刻的快照。

**为什么需要它**：某网关服务滚动发布时有两个说不清的偶发现象：一是 `shutdownNow()` 之后日志里仍有一条任务打印了"处理完成"；二是极少情况下 `awaitTermination(60s)` 一直返回 false，进程被 SIGKILL，可 `getPoolSize()` 明明已经是 0。两次排查都落到同一个窗口上——提交线程"读状态"和"加人数"是两步。它先读到 RUNNING、`workerCount=7`，判断"没到 corePoolSize，可以加人"，然后被调度走了；关闭线程在这期间把状态推到 STOP、清空队列、`tryTerminate` 看到 `workerCount != 0` 直接返回；提交线程回来把人数改成 8 并启动线程，这个线程就带着一个本该被丢弃的任务跑了起来。反过来，如果加人数和减人数不是原子的，`tryTerminate` 里那句 `workerCountOf(c) == 0` 就可能读到一个从没存在过的中间值，池子要么提前终止、要么永远终止不了。

这一章要解决的是：ThreadPoolExecutor 用什么数据结构，把"五种状态"和"几百个 worker"塞进一次原子读写里；以及 Worker 为什么不是裸 Thread，而要继承 AQS。

**核心结构**：

| 概念 | 作用 |
|---|---|
| `ctl` | 唯一的 `AtomicInteger`。高 3 位 runState，低 29 位 workerCount，一次 CAS 同时改两者 |
| `COUNT_BITS = 29` | `Integer.SIZE - 3`。低位宽度，决定 CAPACITY，也决定"为什么不是 30+2" |
| `CAPACITY` | `(1 << 29) - 1` = 536870911。workerCount 的物理上限，同时充当取值掩码 |
| `runStateOf(c)` | `c & ~CAPACITY`，把低 29 位抹掉，只留状态 |
| `workerCountOf(c)` | `c & CAPACITY`，把高 3 位抹掉，只留人数 |
| `ctlOf(rs, wc)` | `rs \| wc`。两段不重叠，或运算等价于拼装且永不进位 |
| 五个状态常量 | RUNNING=-1<<29、SHUTDOWN=0、STOP、TIDYING、TERMINATED，数值单调递增 |
| `mainLock` | `ReentrantLock`，保护 `workers` 集合、`largestPoolSize`、`completedTaskCount`，也是"提交"与"关闭"的串行化点 |
| `workers` | `HashSet<Worker>`，只在 mainLock 下增删；遍历它做中断是关闭流程的全部依据 |
| `Worker` | 继承 AQS 的 Runnable：一个线程 + 一把不可重入的锁 + 一个 firstTask |
| `completedTasks` | `volatile long`，每跑完一个任务 +1，退出时汇总进 `completedTaskCount` |

**关键代码**：

```java
// 一个 int，两段互不重叠的位域
private final AtomicInteger ctl = new AtomicInteger(ctlOf(RUNNING, 0));

private static final int COUNT_BITS = Integer.SIZE - 3;        // 29
private static final int CAPACITY   = (1 << COUNT_BITS) - 1;   // 000 11111...1

// 五种状态占高 3 位。2 位只有 4 个值，装不下 5 个状态，所以必须是 3 位
private static final int RUNNING    = -1 << COUNT_BITS;  // 111 000...
private static final int SHUTDOWN   =  0 << COUNT_BITS;  // 000 000...
private static final int STOP       =  1 << COUNT_BITS;  // 001 000...
private static final int TIDYING    =  2 << COUNT_BITS;  // 010 000...
private static final int TERMINATED =  3 << COUNT_BITS;  // 011 000...

private static int runStateOf(int c)     { return c & ~CAPACITY; } // 只留高 3 位
private static int workerCountOf(int c)  { return c &  CAPACITY; } // 只留低 29 位
private static int ctlOf(int rs, int wc) { return rs | wc; }       // 用 | 不用 +：不进位
```

```java
private final class Worker extends AbstractQueuedSynchronizer implements Runnable {
    final Thread thread;              // 真正的载体，由 ThreadFactory 创建
    Runnable firstTask;               // 只为"新线程别先去抢队列"服务，跑完就置 null
    volatile long completedTasks;

    Worker(Runnable firstTask) {
        setState(-1);                 // 先置 -1：此时 tryLock 必失败，谁都中断不了它
        this.firstTask = firstTask;
        this.thread = getThreadFactory().newThread(this);
    }

    protected boolean tryAcquire(int unused) {
        if (compareAndSetState(0, 1)) {          // 只认 0 -> 1，重入直接被拒
            setExclusiveOwnerThread(Thread.currentThread());
            return true;
        }
        return false;
    }
    protected boolean tryRelease(int unused) {
        setExclusiveOwnerThread(null);
        setState(0);                             // 无条件归零，-1 也一并抹掉
        return true;
    }
}

// 加锁范围 = 执行任务的时间，不含 getTask 的阻塞
w.unlock();                          // state -1 -> 0，从这一刻起允许被中断
while (task != null || (task = getTask()) != null) {
    w.lock();                        // 上锁 == 对外宣告"我正在跑任务"
    try { task.run(); }
    finally { task = null; w.completedTasks++; w.unlock(); }
}
```

**拆解**：

### 为什么是 29 + 3，以及为什么用 `|` 拼

五个状态至少需要 3 个二进制位（2 位只有 4 个组合），剩下的 29 位留给人数，上限 5.3 亿——远超任何机器的线程承载能力，也远超内核 pid 空间。这个切分的依据不是"省内存"，而是"剩下的位够不够用"：如果状态只有 4 种，Doug Lea 也完全可以切 30 + 2。

拼装用 `|` 而不是 `+` 是个容易看漏的细节。两段位域不重叠时两者结果相同，但 `|` 在语义上表达的是"覆盖"，即便将来 `wc` 用满了 29 位也不会进位污染状态位。同理，取值用掩码 `& ~CAPACITY`（`~CAPACITY` = `0xE0000000`）而不是右移，是因为右移会把 RUNNING 的符号位搞成算术右移的填充问题，而掩码对正负一视同仁。

为什么不用 `AtomicLong` 切成 32 + 32？因为 29 位已经够用（5 亿线程），而 32 位整数的 CAS 是绝大多数架构上单指令就能完成的原子操作；64 位的 `AtomicLong` 在 32 位平台上要靠 `LockCmpxchg8b` 甚至加锁实现，为了一个用不到的容量把最热的路径变慢，不划算。位宽的选择从来是被硬件约束推着走的。

### 五种状态：为什么 RUNNING 取 -1，为什么能直接比大小

状态机本身很简单：RUNNING 收新任务也处理队列；SHUTDOWN 不收新任务但把队列跑完；STOP 不收也不跑，还要中断正在跑的；TIDYING 是"人已经清空、马上要收尾"的过渡态，此时会调 `terminated()` 钩子；TERMINATED 是钩子执行完的终态。

真正精巧的是取值。RUNNING 定义为 `-1 << 29`，也就是 `0xE0000000`，符号位是 1——任何 RUNNING 状态的 ctl 都是负数。SHUTDOWN 是 0，STOP 及以上是正数。于是：

- `isRunning(c)` 写成 `c < SHUTDOWN`，一句整数比较，不用拆位；
- `runStateAtLeast(c, STOP)` 写成 `c >= STOP`，也不用拆位。

这依赖两个前提：状态常量必须单调递增，且 workerCount 永远非负。后者不是天然成立的——`decrementWorkerCount` 用的是 CAS 循环，一旦有人多减一次，人数变成 -1，低 29 位全 1，`c >= STOP` 会误判，整个状态机就崩了。所以那个 CAS 循环里的断言、以及 `addWorkerFailed` 的补偿路径，都不是防御性编程，是这套位技巧的前置条件。

转换条件只有五条，记住触发点即可：`shutdown()` 推 RUNNING→SHUTDOWN；`shutdownNow()` 推 RUNNING/SHUTDOWN→STOP；SHUTDOWN 下"队列空且人数为 0"→TIDYING；STOP 下"人数为 0"→TIDYING；`terminated()` 返回后 TIDYING→TERMINATED。所有推进都收口在 `tryTerminate()` 一个方法里，而 `tryTerminate()` 在每一个可能让"人数归零"或"队列变空"的地方被调用——这是后一章的主线。

把 ctl 当成一条数轴，这五个状态就变成五个刻度。一个 RUNNING、7 个 worker 的池子，ctl 是 `0xE0000007`；`shutdown()` 之后变成 `0x00000007`——注意人数那 7 位一个都没动，只有高 3 位从 `111` 翻成 `000`，所以状态迁移和人数变化在同一个值里同时可见。再 `shutdownNow()` 变成 `0x20000007`。等 worker 一个个退完，`0x20000000`，进 TIDYING 是 `0x40000000`，`terminated()` 跑完是 `0x60000000`，人数位清零。任何时刻你只要打印 `ctl` 的十六进制，就能一眼读出"什么状态、几个人"，这是这套编码额外的红利，调试线上问题时比 `toString()` 管用。

SHUTDOWN 和 STOP 的分界，不是"收不收新任务"，而是"已经进了队列的任务还算不算数"。这个区别直接决定了一次发布会不会丢任务：`shutdown()` 之后队列里剩下的几千个任务会被现有 worker 全部跑完，`shutdownNow()` 之后它们被 `drainQueue()` 捞出来变成返回值，如果调用方不看返回值、也不重新提交，这些任务就永久消失了。所以两个方法名字只差一个 Now，语义差的是"数据要不要保住"。

### 为什么必须是一个 AtomicInteger，而不是两个字段

假设拆成 `volatile int runState` 和 `AtomicInteger workerCount`。提交路径要做的是"确认状态允许 + 占一个名额"，这两步要么加锁，要么就会出现前面那个窗口：状态检查通过之后、名额占用之前，池子被关了。而加锁意味着每次 `execute` 都要抢 `mainLock`，线程池最热的路径直接退化成串行。

一个 int 之后，`compareAndIncrementWorkerCount(c)` 的语义变成了"状态仍是这个状态，并且人数成功 +1"，一次 CAS 完成两件事。失败时也带着信息：重新读 ctl，如果 `runStateOf(c) != rs`，说明状态被动过，之前所有的判断全部作废，必须回到外层循环重来；如果状态没变，那只是名额被别人抢了，内层重试即可。`addWorker` 那个两层循环的结构，本质上就是这个"一次 CAS 携带两个维度"的直接产物。

反过来看，两个字段还有一个更隐蔽的代价：读者会看到不存在的状态组合。监控线程读到 `runState = STOP`、再读到 `workerCount = 9`，这个组合在系统里从未出现过——关闭线程是先推状态再逐个中断的，人数在那一刻可能是 7 也可能是 8。用 ctl 之后，`getPoolSize()`、`getActiveCount()`、`isShutdown()` 全部来自同一次 `ctl.get()`，至少互相自洽。

代价是 ctl 只能表达 32 位的信息，所以"哪些 worker"这件事塞不进去，必须另开一个 `HashSet` 用 `mainLock` 保护。这就是这套设计的两层结构：**计数走无锁 CAS，集合走全局锁**。前者是每次提交都要碰的热路径，后者只在创建/销毁线程时碰。

这个分工在调用链上看得最清楚。`execute` 全程只读一次 `ctl.get()`，加上 `addWorker` 里的一次 CAS，就完成了"接不接这个任务"的决定，不碰任何锁；`addWorker` 的第二阶段才拿 `mainLock`，此时已经确定要建线程了，锁竞争的概率极低。而关闭方向刚好相反，`shutdown()` 里 `advanceState(SHUTDOWN)` 是一次 ctl 的 CAS（这一步不需要 mainLock，因为它只改状态位），随后 `interruptIdleWorkers` 必须拿 `mainLock` 才能安全遍历 `workers`——集合在并发增删，不拿锁遍历会直接 `ConcurrentModificationException`。于是"提交"和"关闭"真正串行化的地方只有一处：`addWorker` 拿锁复查状态的那几行。其余全是 CAS 和 volatile 读，这是线程池在 10 万 QPS 下仍然不成为瓶颈的原因。

### Worker 继承 AQS 图什么

不是为了互斥——每个 Worker 一把锁，彼此毫不相干，没有共享资源要保护。它是把一个 boolean 标志位做成了带排队语义的锁，而这个标志位的含义是"这个线程此刻正在执行任务"。

三个地方依赖它：

第一，`runWorker` 在 `task.run()` 前后成对 `lock()/unlock()`，锁的范围精确等于一次任务执行，**不包含** `getTask()` 里阻塞在 `workQueue.take()` 的那段时间。于是"持有锁"就等于"在干活"，"不持有锁"就等于"空闲或正在等任务"。

第二，`shutdown()` 里的 `interruptIdleWorkers` 靠 `w.tryLock()` 判断空闲：抢到锁说明这个 worker 没在跑任务，可以安全中断；抢不到就跳过，正在跑的任务不能被打断。整个"优雅关闭"的语义就建立在这一个 tryLock 上。

第三，不可重入是关键。`tryAcquire` 只接受 `0 -> 1`，同一个线程再次 `lock()` 会失败。如果这里用的是 `ReentrantLock`，一个任务里调 `shutdown()` 就会走成：shutdown → interruptIdleWorkers → 对自己的 worker `tryLock()` → 因为是重入所以成功 → `t.interrupt()` 打断自己。不可重入让这条路径自然失效，任务里关自己的池子是安全的。

构造器里的 `setState(-1)` 是同一个思路的延伸：线程对象已经建好但还没 `start()`，这个窗口里 `tryLock` 必须失败，否则关闭流程会去中断一个尚未启动的线程。`runWorker` 开头那句 `w.unlock()` 把 -1 归零，注释写的是 `allow interrupts`——从这里开始，它才是一个可以被关闭流程看见的 worker。

那为什么不用一个 `volatile boolean busy` 呢？因为这里需要的是 test-and-set：`interruptIdleWorkers` 必须原子地完成"判断空闲并占住"，否则它判定为空闲、刚准备 `interrupt()`，线程恰好取到任务开始执行，中断就打在了任务身上。`tryLock()` 的 CAS 把判定和占用合成一步，`unlock()` 再把所有权释放。同时 AQS 的 acquire/release 自带内存语义：`unlock()` 之前的写（`completedTasks++`、任务对共享状态的修改）对下一个 `lock()` 的线程可见，池子不需要额外的屏障。用一把"锁"实现一个布尔标志，换来的是原子性和可见性两样东西。

顺带一提，线程池那几个监控方法几乎全部建立在这把锁上：`getActiveCount()` 遍历 workers 数 `isLocked()` 的个数，`getPoolSize()` 直接 `workers.size()`，`getLargestPoolSize()` 读 `largestPoolSize`（只在 mainLock 下更新）。所以 `getActiveCount()` 数的是"正在 `task.run()` 里"的线程，不含阻塞在 `take()` 上的空闲线程。这解释了一个常见困惑：池子明明被下游 IO 卡死了，`getActiveCount()` 却是满的、CPU 只有 15%——活跃不等于在算，锁只标记"有没有在跑任务"，不区分跑得快不快。

**容易误解的地方**：

1. **"低 29 位是活着的线程数"**。它是 Worker 对象的数量。`getTask()` 在返回 null 之前就已经 `decrementWorkerCount()` 了，所以存在一个短暂窗口：人数已经减掉，线程还在栈上没退完。`getPoolSize()` 和 jstack 里 `pool-1-thread-N` 的条数对不上是正常的，不是 bug。

2. **"RUNNING 就是 0"**。是 -1。写成 0 的话，`isRunning(c) = c < SHUTDOWN` 永远为 false，池子一创建就是关闭状态。这个 -1 同时承担了"符号位让所有 RUNNING 的 ctl 为负"的职责，换成任何正数都会让直接比较失效。

3. **"Worker 的锁是给多个 worker 互斥用的"**。每个 Worker 是 AQS 的独立实例，state 各管各的。它保护的不是共享数据，而是"我在跑任务"这一个事实。真正保护 `workers` 集合的是 `mainLock`，两者别混。

4. **"`allowCoreThreadTimeOut(true)` 是把 keepAliveTime 调小"**。它不动 keepAliveTime，改的是 `getTask()` 里 `timed = allowCoreThreadTimeOut || wc > corePoolSize` 这个判定，让核心线程从 `take()` 死等变成 `poll(keepAliveTime)` 限时等。同一个 keepAliveTime，语义完全不同。

**记住这一句**：ctl 不是省内存的位运算杂技，它是把"状态迁移"和"人数变化"压成一次 CAS 的最小临界区。
