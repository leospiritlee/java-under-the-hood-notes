# 第9章 线程池：任务从提交到执行的全过程

**一句话**：execute 只有三条路——加人、入队、拒绝，坑全在"先入队再扩容"这个顺序上。

**为什么需要它**：一个报表导出接口用了 `Executors.newFixedThreadPool(8)`。上线三个月相安无事，某天大客户一次点了 2000 个导出，每个任务要扫 30 万行。第二天堆 dump 里 40 万个 `FutureTask` 加 40 万个 `LinkedBlockingQueue$Node` 占着 3.2G，Full GC 两秒一次，最后 `OutOfMemoryError: Java heap space`。诡异的是现场指标：线程数始终 8，CPU 15%，`getActiveCount()` 恒等于 8，`getQueue().size()` 是 39 万。没有人配错参数——`corePoolSize = maximumPoolSize = 8` 看起来天经地义，问题出在这套参数遇上无界队列之后，`maximumPoolSize`、`keepAliveTime`、拒绝策略三者同时变成了死配置。

这一章走一遍从 `submit` 到 `task.run()` 再到线程退场的完整链路，重点在每一个分叉的判断依据，以及每条异常路径最后落到哪里。

**核心结构**：

| 概念 | 作用 |
|---|---|
| `execute(Runnable)` | 唯一入口。`submit` 只是把任务包成 FutureTask 再调它 |
| `addWorker(task, core)` | 真正创建线程。`core` 只影响上限用 corePoolSize 还是 maximumPoolSize |
| `Worker.firstTask` | 新线程的第一个任务。避免刚建好就去队列抢，省一次上下文切换 |
| `getTask()` | worker 的空转循环。返回 null 就是"这个线程该退出了" |
| `processWorkerExit` | 退场收尾：补记账、移出集合、按需补人、试探终止 |
| `tryTerminate()` | 唯一把状态推进到 TIDYING/TERMINATED 的地方 |
| `reject()` → `RejectedExecutionHandler` | 四条内置策略：Abort / CallerRuns / Discard / DiscardOldest |
| `advanceState()` | CAS 推进状态，只许往前走，`shutdown` 与 `shutdownNow` 共用 |
| `interruptIdleWorkers` / `interruptWorkers` | 前者靠 `w.tryLock()` 只打空闲的，后者无条件全打 |
| `drainQueue()` | `shutdownNow` 专用，把队列里没跑的任务捞出来返回 |

**关键代码**：

```java
public void execute(Runnable command) {
    int c = ctl.get();
    // ① 人没招满 core，直接开线程，任务当 firstTask，不经过队列
    if (workerCountOf(c) < corePoolSize) {
        if (addWorker(command, true)) return;
        c = ctl.get();                       // 加人失败（并发抢名额或状态变了），重读再判
    }
    // ② 池子在跑，就往队列里塞。注意：能进到这里说明 core 已经满了
    if (isRunning(c) && workQueue.offer(command)) {
        int recheck = ctl.get();
        if (!isRunning(recheck) && remove(command))   // 入队后发现池子关了，捞回来拒绝
            reject(command);                          // remove 失败说明已被 worker 取走，不拒
        else if (workerCountOf(recheck) == 0)         // 队列有活但一个人都没有：补一个
            addWorker(null, false);
    }
    // ③ 队列满了（或池子已关），才尝试突破 core 往 max 扩
    else if (!addWorker(command, false))
        reject(command);                              // 扩不动了，走拒绝策略
}
```

```java
private Runnable getTask() {
    boolean timedOut = false;
    for (;;) {
        int c = ctl.get();
        int rs = runStateOf(c);
        // 出口一：关闭中且（已 STOP 或队列已空）
        if (rs >= SHUTDOWN && (rs >= STOP || workQueue.isEmpty())) {
            decrementWorkerCount();          // 先减人，再退出，顺序不能反
            return null;
        }
        boolean timed = allowCoreThreadTimeOut || workerCountOf(c) > corePoolSize;
        // 出口二：setMaximumPoolSize 调小后，多出来的人自杀
        if (workerCountOf(c) > maximumPoolSize || (timedOut && timed)) {
            if (compareAndDecrementWorkerCount(c)) return null;
            continue;
        }
        try {
            // 出口三在 catch 里：被中断不算退出，重来一轮重新评估 timed
            Runnable r = timed ? workQueue.poll(keepAliveTime, NANOSECONDS)
                               : workQueue.take();
            if (r != null) return r;
            timedOut = true;                 // 这一轮超时了，下一轮直接走出口二
        } catch (InterruptedException retry) {
            timedOut = false;                // 缩容中断 / 关闭中断，都不该直接死
        }
    }
}

// 线程退场后决定要不要补人
if (runStateLessThan(c, STOP)) {
    if (!completedAbruptly) {                // 正常退出才看下限
        int min = allowCoreThreadTimeOut ? 0 : corePoolSize;
        if (min == 0 && !workQueue.isEmpty()) min = 1;   // 有活干就至少留一个
        if (workerCountOf(c) >= min) return;
    }
    addWorker(null, false);                  // 异常退出则无条件补，受 max 限制
}
```

**拆解**：

### 三段判断，以及"先入队再扩容"埋的坑

把参数设成 `core=8, max=64, queue=ArrayBlockingQueue(100)`，提交 200 个任务，实际发生的是：前 8 个直接开线程；第 9 到 108 个全部进队列；第 109 个才发现 `offer` 返回 false，此时才 `addWorker(command, false)` 把线程数从 8 往上顶。也就是说，线程数从 8 涨到 64 这件事，被推迟到队列彻底塞满之后才发生。

这个顺序不是随手写的。线程是这里最贵的资源——一个线程默认 1M 栈、一次 `clone()` 系统调用、还要参与内核调度；队列是预先分配好的内存，入队就是一次 CAS。Doug Lea 的假设是：**队列用来吸收正常波动，扩容是用来兜住异常的**。所以先试便宜的手段，再试贵的。

但它带来两个必须知道的后果。

第一个后果：**无界队列让第③段成为死代码**。`LinkedBlockingQueue()` 无参构造的容量是 `Integer.MAX_VALUE`，`offer` 几乎永远返回 true，`addWorker(command, false)` 永远执行不到。于是 `maximumPoolSize`、`keepAliveTime`、拒绝策略三者全部失效——池子的实际并发度被钉死在 `corePoolSize`，队列长度变成了唯一的缓冲，而它没有上限。`newFixedThreadPool(n)` 和 `newSingleThreadExecutor()` 都是这个形态，它们的真名应该叫"n 个线程 + 一个无限长的待办清单"。任务比线程快，清单就无限长，直到堆爆掉。这不是参数配错，是 API 的默认值把"背压"这个机制整个删掉了。

第二个后果：**有界队列下，延迟先恶化，然后才扩容**。直觉上"忙不过来就加人"，实际是"忙不过来就先排队，排到装不下才加人"。队列设 10000、max 设 64 的池子，在突发流量下的表现是：8 个线程跑，10000 个任务排队，第 10009 个任务提交时才想起可以扩到 9 个线程。用户看到的 P99 从 50ms 涨到 8 秒，而 CPU 还有 70% 空闲。想要"快速扩容"的语义，得反过来配：小队列（几十）+ 大 maxPool，让 `offer` 尽早失败，逼出扩容。

第②段里的 `recheck` 是另一个容易读漏的点。入队成功后重新读一次 ctl，是因为 `offer` 和状态检查之间池子可能被关了。此时要把任务捞回来拒绝——但只有 `remove(command)` 成功才拒。如果 remove 失败，说明任务已经被某个 worker 取走开始执行了，这时候再拒绝就是"既执行了又报错"，所以宁可放过。

### addWorker：两层 CAS 循环与主锁内的二次校验

`addWorker` 的第一个循环管状态，第二个循环管名额。

外层判断是个拗口的条件：`rs >= SHUTDOWN && !(rs == SHUTDOWN && firstTask == null && !workQueue.isEmpty())` 就返回 false。翻译过来：**STOP 及以上一律不许加人；SHUTDOWN 下只有"没有新任务要带、且队列里还有活"这一种情况允许加人**。这正是 SHUTDOWN 的语义——不收新活，但得有人把存货干完。所以 `shutdown()` 之后队列里还剩 100 个任务、线程却全因超时退光了，会由 `processWorkerExit` 补人，靠的就是这个分支。

内层先比 `wc >= (core ? corePoolSize : maximumPoolSize)`，再 CAS。CAS 的对象是完整的 ctl，所以它同时校验了"状态没变"和"名额 +1 成功"。失败后的分流是关键：`if (runStateOf(c) != rs) continue retry;`——状态被动了，前面所有判断作废，回到外层重新看状态；否则只是名额被抢，内层重试。这个两层结构让"状态稳定时的竞争"只付出一次 CAS 重试的代价，不会每次都重走状态检查。

CAS 成功之后并没有结束。代码要 `new Worker(firstTask)`（一次对象分配 + 一次 ThreadFactory 调用），然后拿 `mainLock`，**在锁里再查一次状态**：`rs < SHUTDOWN || (rs == SHUTDOWN && firstTask == null)`。这次复查是必需的，因为从 CAS 到 `t.start()` 之间隔了一次线程创建，`shutdown()` 完全可能在这期间跑完整个流程。`mainLock` 是提交与关闭的公共串行化点——`shutdown()` 也要拿它才能遍历 `workers` 做中断——所以拿着它复查，就等于把"加人"和"关闭"这两件事排了序。

`t.start()` 放在 `mainLock` 外面，是因为 `ThreadFactory` 是用户代码，可能很慢甚至阻塞，不能拿着全局锁调。

任何一步失败都走 `addWorkerFailed`：从 `workers` 移除、`decrementWorkerCount()`、`tryTerminate()`。三步缺一不可，尤其第三步。如果这次 add 恰好是让 `wc` 从 0 变 1 的那一次，而关闭流程刚刚在 `wc == 0` 的判断上走开了，那么不加这一次 `tryTerminate`，池子就永远停在 SHUTDOWN——这正是第 8 章开头那个"关不掉"的现象。

### getTask：一次阻塞读，三个返回 null 的出口

`getTask` 是 worker 的一生。它只有三种结局：拿到任务、超时退出、被状态赶走。

出口一是关闭路径：`rs >= SHUTDOWN && (rs >= STOP || workQueue.isEmpty())`。注意括号——STOP 及以上不看队列，SHUTDOWN 要看队列空不空。这里 `decrementWorkerCount()` 在 `return null` 之前，所以"人数已减、线程未死"的窗口是设计出来的，不是竞态。

出口二是缩容路径：`workerCountOf(c) > maximumPoolSize`。这个分支只有在有人调了 `setMaximumPoolSize` 把上限调小时才会命中，多出来的 worker 通过 CAS 减人后自杀。

出口三是超时路径：`timedOut && timed`。`timedOut` 是上一轮的遗产——上一轮 `poll` 超时返回 null，置为 true，下一轮进循环时立刻命中出口二的条件，减人退出。为什么不在超时那一轮直接退出？因为从 `poll` 返回到再次判断之间，可能刚有任务进来，多给一次机会的成本只是一次循环。

`timed` 每轮重算：`allowCoreThreadTimeOut || wc > corePoolSize`。所以同一个线程，在池子满编时是 `take()` 死等，在池子超编时是 `poll(keepAliveTime)` 限时等——行为随池子规模漂移，这是很多人没意识到的。

`catch (InterruptedException)` 里只置 `timedOut = false` 然后继续循环，不退出。这个选择对应两类中断：`setMaximumPoolSize` 缩容时会中断空闲 worker，让它们重新评估自己是否超编；`shutdown` 时中断后由循环开头的状态检查兜住。如果这里直接 `return null`，缩容就会变成"误杀"，本来该留下的核心线程也被干掉了。

### 退场之后：processWorkerExit、四种拒绝策略与两种关闭

`runWorker` 的 `finally` 里调 `processWorkerExit(w, completedAbruptly)`。`completedAbruptly` 为 true 表示 `task.run()` 抛出的异常穿透了 while 循环——这种情况下 `getTask` 没来得及减人，所以 `processWorkerExit` 第一件事就是补 `decrementWorkerCount()`。这就是"任务抛异常不会让池子少一个线程"的机制：人减了，但紧接着的补人逻辑又加回来一个。代价是线程 churn，以及异常本身对 `execute` 的提交者完全静默——它只会走到线程的 `UncaughtExceptionHandler`，用 `submit` 提交才会被 `Future.get()` 重新抛出。

补人的条件值得单独看：正常退出时看下限 `min`（`allowCoreThreadTimeOut` 时是 0，否则是 corePoolSize，队列非空时至少 1），人够了就不补；**异常退出时跳过下限判断，无条件补一个**（仍受 max 限制）。所以"任务抛异常"和"任务正常跑完、线程超时退出"走的是两条不同的补人路径。

`tryTerminate` 是终止的唯一入口，条件很严：状态必须 ≥ SHUTDOWN，不能是"SHUTDOWN 但队列还有货"，人数必须为 0。人数不为 0 时它不是直接返回，而是 `interruptIdleWorkers(ONLY_ONE)`——叫醒一个空闲 worker，让它自己走到 `getTask` 的出口一再调一次 `tryTerminate`。这是级联唤醒，避免所有 worker 同时醒来抢锁。理解这一点，才能理解为什么 `addWorkerFailed`、`processWorkerExit`、`shutdown` 三个地方都要调它：任何一次"人数可能归零"的动作，都得负责把终止检查接力下去。

四种拒绝策略的差别在于"任务最后落在谁手上"：

| 策略 | 行为 | 适用 |
|---|---|---|
| `AbortPolicy`（默认） | 抛 `RejectedExecutionException` | 宁可失败也不能丢，调用方有能力处理 |
| `CallerRunsPolicy` | 池子未关闭时在**提交线程**上 `r.run()` | 天然反压：提交者被占住，自然不会继续提交 |
| `DiscardPolicy` | 空实现，静默丢弃 | 可丢的埋点、日志 |
| `DiscardOldestPolicy` | `queue.poll()` 扔掉队头，再 `execute(r)` | 只要最新值，如行情推送 |

`CallerRunsPolicy` 的反压是真的有效——它把"队列满"这个信号直接变成了"提交变慢"。但它也很危险：如果提交者是 Tomcat 的请求线程，你等于把积压从线程池搬到了 Web 容器，最终拖垮的是整个接入层。`DiscardOldestPolicy` 则有个致命细节：它先 `poll()` 再 `execute()`，配 `SynchronousQueue` 时 `poll()` 永远返回 null，`execute()` 又会走到拒绝，于是无限递归，直到 `StackOverflowError`。

最后是两种关闭。`shutdown()` 做三件事：`advanceState(SHUTDOWN)`、`interruptIdleWorkers()`（靠 `w.tryLock()` 只打空闲的）、`tryTerminate()`。`shutdownNow()` 做三件事：`advanceState(STOP)`、`interruptWorkers()`（不看锁，全打）、`drainQueue()` 把没开始的任务捞出来返回。关键差别有两个：`shutdownNow` 清空队列，返回的 `List<Runnable>` 就是那些从未执行的任务，调用方有责任处理；以及它**只是发中断**，任务是否真的停下取决于任务自己有没有检查中断标志——一个死循环里不响应中断的任务，`shutdownNow` 也拿它没办法。

**容易误解的地方**：

1. **"线程数到了 corePoolSize 就开始往 max 扩"**。不是。是队列满了才扩。无界队列下永远不会扩，`maximumPoolSize` 是个装饰品。这也是 `newFixedThreadPool` 把 core 和 max 设成相等的原因——反正也扩不了，不如写得诚实。

2. **"任务抛异常会让线程池少一个线程"**。不会，`processWorkerExit` 会补一个。但异常被吞掉了：`execute` 提交时没有 Future 承接，只有 `ThreadFactory` 设的 `UncaughtExceptionHandler` 能看到。生产上表现为"任务莫名消失，日志里啥也没有"。

3. **"`shutdownNow` 会停掉正在执行的任务"**。它只是 `interrupt()`，外加清空队列。正在跑的任务如果不响应中断，会一直跑到自然结束；`awaitTermination` 也就一直等不到。

4. **"`DiscardOldestPolicy` 是安全的降级方案"**。它在 `SynchronousQueue` 上会无限递归直到栈溢出，因为 `poll()` 对容量 0 的队列永远返回 null，而 `execute()` 又会再次进入拒绝分支。用之前先看队列类型。

**记住这一句**：线程池的真实容量不是 `maximumPoolSize`，是"队列能装多少"加上"线程跑得多快"，参数只是这两者的伪装。
