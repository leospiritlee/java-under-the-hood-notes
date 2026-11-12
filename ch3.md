# 第3章 AQS 骨架：一个 int 和一个双向队列

**一句话**：一个 volatile int 记锁状态，一条双向队列排线程，park/unpark 负责挂起与唤醒。

**为什么需要它**：先看一个几乎所有人都写过的错误实现。想自己造一把锁，最直觉的写法是自旋 CAS：

```java
public void lock() {
    while (!UNSAFE.compareAndSwapInt(this, stateOffset, 0, 1)) {
        // 空转，什么都不做
    }
}
```

单核两线程时它工作得很好，所以本地测不出问题。上到 32 核、200 个线程争同一把锁时，问题立刻暴露：抢不到的那 199 个线程全在 CPU 上烧指令，真正的业务线程分不到时间片，吞吐随并发数上升反而下降。改成 `Thread.sleep(1)` 让出 CPU，又换来三个新问题：唤醒延迟被固定成毫秒级、唤醒顺序完全随机、无法实现 `tryLock(timeout)`。再退一步用 `Object.wait/notify`，`notify` 只能随机唤醒一个、不能指定唤醒谁、不能在等待前判断条件是否已满足，更没法表达"许可数"这种非互斥语义。

推演一下"如果没有 AQS"：`ReentrantLock` 要写一遍排队、`Semaphore` 要写一遍、`CountDownLatch`、`ReentrantReadWriteLock`、`ThreadPoolExecutor` 的 `mainLock`、`FutureTask` 各自都要写一遍。而这条路径上每一处都是坑——CAS 失败后的重试、节点取消后的链表清理、`park` 与 `unpark` 的时序、中断语义、重入计数。Doug Lea 的选择是把这套"抢不到怎么办"的机制全部抽出来做成模板，只留一个钩子给子类：**state 怎么解释、此刻能不能拿**。这就是 AQS 的全部设计意图——它不实现锁，它实现排队。

本章要回答四件事：一个 int 凭什么够用、`waitStatus` 的四个值到底描述谁、队列为什么必须是双向的、`acquire` 的三步各自在防什么。

**核心结构**：

| 概念 | 作用 |
|---|---|
| `volatile int state` | 同步状态。AQS 只负责存和 CAS，语义完全由子类定义 |
| `exclusiveOwnerThread` | 独占模式下当前持锁线程，仅用于重入判定与 unlock 校验 |
| `head` / `tail` | 队列首尾。head 是哨兵，永远不持有线程 |
| `Node.thread` | 被包装的线程；head 上恒为 null |
| `Node.prev` / `next` | 双向链表指针。`prev` 可靠，`next` 是"尽力而为"的 |
| `Node.waitStatus` | 0 / CANCELLED(1) / SIGNAL(-1) / CONDITION(-2) / PROPAGATE(-3) |
| `Node.nextWaiter` | 条件队列的单向指针，与 `next` 完全无关 |
| `tryAcquire(int)` | 子类钩子：唯一一次"试试能不能拿"的机会，失败返回 false |
| `addWaiter(Node)` | 把当前线程包装成节点，走快路径入队，失败退到 `enq` 自旋 |
| `acquireQueued(Node,int)` | 队列中的主循环：判断是否轮到自己 → 决定是否 park → 醒来重试 |
| `shouldParkAfterFailedAcquire` | 把前驱置为 SIGNAL，返回"现在能不能安全 park" |
| `parkAndCheckInterrupt` | `LockSupport.park` 的唯一调用点，返回时清中断标记 |
| `release(int)` / `unparkSuccessor` | 释放状态并唤醒 head 之后第一个未取消的节点 |

**关键代码**：

```java
// AQS.acquire：三步走，失败才排队
public final void acquire(int arg) {
    if (!tryAcquire(arg) &&                       // 第一步：先试。成功则整个方法结束，
                                                  // 队列都不用建，这是绝大多数情况
        acquireQueued(addWaiter(Node.EXCLUSIVE), arg))  // 第二步入队，第三步在队里等
        selfInterrupt();                          // 排队期间被中断过，这里只补标记，不抛异常
}

// 子类钩子。AQS 里它是空实现，直接抛 UnsupportedOperationException
protected boolean tryAcquire(int arg) {
    throw new UnsupportedOperationException();
}
```

```java
final boolean acquireQueued(final Node node, int arg) {
    boolean failed = true;
    try {
        boolean interrupted = false;
        for (;;) {                                 // 死循环，唯一的出口是抢到锁
            final Node p = node.predecessor();
            if (p == head && tryAcquire(arg)) {    // 只有前驱是 head 才有资格试
                setHead(node);                     // 自己变成新哨兵：thread=null, prev=null
                p.next = null;                     // 断开旧哨兵，help GC
                failed = false;
                return interrupted;
            }
            if (shouldParkAfterFailedAcquire(p, node) &&  // 问：现在能安全挂起吗
                parkAndCheckInterrupt())                  // 能，才真的挂起
                interrupted = true;                       // 醒来只知道"被中断过"，继续抢
        }
    } finally {
        if (failed) cancelAcquire(node);           // tryAcquire 抛异常时兜底，标记取消
    }
}

private static boolean shouldParkAfterFailedAcquire(Node pred, Node node) {
    int ws = pred.waitStatus;
    if (ws == Node.SIGNAL)
        return true;                               // 前驱已经承诺唤醒我，可以放心 park
    if (ws > 0) {                                  // 前驱取消了，往前找一个活的
        do {
            node.prev = pred = pred.prev;          // 顺手把取消节点从链上摘掉
        } while (pred.waitStatus > 0);
        pred.next = node;
    } else {
        // 前驱是 0（新建节点都是 0），CAS 成 SIGNAL 表示"我可能要你唤醒"
        // 注意这里返回 false，调用方会再转一圈 for 循环
        compareAndSetWaitStatus(pred, ws, Node.SIGNAL);
    }
    return false;                                  // 第一次进来几乎必然返回 false
}

private void unparkSuccessor(Node node) {          // 入参是 head
    int ws = node.waitStatus;
    if (ws < 0)
        compareAndSetWaitStatus(node, ws, 0);      // 唤醒责任已履行，状态清零
    Node s = node.next;
    if (s == null || s.waitStatus > 0) {           // next 不可靠：可能为 null，可能已取消
        s = null;
        for (Node t = tail; t != null && t != node; t = t.prev)
            if (t.waitStatus <= 0) s = t;          // 从尾部倒着找，找到最靠前的活节点
    }
    if (s != null) LockSupport.unpark(s.thread);
}
```

**拆解**：

### 一、state：一个 int，四种语义

很多人把 `state` 直接等同于"锁计数"，这只在 `ReentrantLock` 里成立。AQS 自己从不读 state 的含义，它只提供三个 `protected` 方法：`getState()`、`setState()`、`compareAndSetState()`。同一个字段在不同子类里是完全不同的东西：

- `ReentrantLock`：0 空闲，1 被占，n 表示重入 n 层。
- `Semaphore`：剩余许可数，`acquire` 是减，`release` 是加。
- `CountDownLatch`：还剩几次 `countDown`，减到 0 就永久放行。
- `ReentrantReadWriteLock`：高 16 位存读锁持有数，低 16 位存写锁重入数，一次 CAS 同时改两个计数。

为什么是 `int` 而不是 `boolean` 或 `long`？`boolean` 表达不了重入和许可数；`long` 在 32 位 JVM 上 CAS 不保证原子（要靠 `cmpxchg8b`，性能和可移植性都更差），而 32 位对这个场景绰绰有余——`ReentrantReadWriteLock` 把 16 位给读、16 位给写，65535 的上限已经远超任何合理用法。

独占模式下 state 的转移只有四种：`0→1` 加锁、`1→2` 重入、`2→1` 释放一层、`1→0` 真正释放并唤醒后继。这个闭环解释了 `ReentrantLock` 最经典的两个 bug：`lock()` 两次只 `unlock()` 一次，state 停在 1，队列里所有线程永久 park；`unlock()` 比 `lock()` 多一次，`tryRelease` 里 `Thread.currentThread() != getExclusiveOwnerThread()` 直接抛 `IllegalMonitorStateException`。`lock/unlock` 必须严格配对不是编码风格问题，是 state 能不能归零的问题。

还有一个容易被忽略的点：`setState` 是普通写，不是 CAS。为什么重入路径可以不用 CAS？因为能走到 `else if (current == getExclusiveOwnerThread())` 的只有持锁线程本身，独占语义保证同一时刻只有一个线程在写 state，不存在竞争。而 `0→1` 那一步必须 CAS，因为可能有一堆线程同时看到 0。判断"要不要 CAS"的标准不是"这个字段可不可变"，而是"此刻有没有别的线程可能同时写它"。

state 的 `volatile` 还承担了内存语义。`unlock` 里的 `setState(0)` 是一次 volatile 写，`lock` 里 `compareAndSetState(0, 1)` 是 volatile 读加写，前者 happens-before 后者，于是 A 在临界区里的所有普通写，对随后拿到锁的 B 全部可见。这就是"锁提供可见性"的底层机制——不是 JVM 对锁有什么特殊照顾，是 state 这个 volatile 字段的读写顺序在起作用。理解了这一点，就明白为什么自定义同步器时，所有状态变更都必须走 `setState` / `compareAndSetState`，而不能自己另开一个普通字段：另开字段就断了这条 happens-before 链，锁还在，可见性没了。

### 二、Node 与 waitStatus：SIGNAL 描述的是别人

`waitStatus` 有五个取值，其中只有一个是正数，这个设计不是偶然：

- `CANCELLED = 1`：节点已放弃。**唯一大于 0 的值**，所以全源码里判断"取消"统一写成 `ws > 0`，一次比较搞定，不需要 `==` 判断。
- `SIGNAL = -1`：**当前节点的后继**处于（或即将进入）park 状态，当前节点释放时必须 unpark 它。
- `CONDITION = -2`：节点在条件队列里等着，只在 `ConditionObject` 内部有效，进同步队列前必须被 CAS 改回 0。
- `PROPAGATE = -3`：共享模式下唤醒需要向后传播，独占模式见不到。
- `0`：初始值。`new Node()` 不设任何状态，所以每个刚入队的节点都是 0。

最反直觉的一点：**SIGNAL 描述的不是自己，是后继**。新节点入队时自己是 0，是它的**前驱**被改成 -1。所以一条正常队列里，除了最后一个节点，几乎每个节点的 `waitStatus` 都是 -1——因为它的后继替它设了。换个说法：每个节点入队后做的第一件事，是回头告诉前面那个人"我要睡了，你释放的时候记得叫我"。

这个约定解决了两个问题。第一，唤醒有了明确的责任人——不是"随便找个线程叫醒"，而是"前驱释放时叫醒它的直接后继"，责任链是局部的，不需要全局锁。第二，`release` 里可以廉价地判断"要不要叫"：`if (h != null && h.waitStatus != 0)`，head 的 ws 是 0 说明后面没人等着（或已经叫过了），直接跳过 `unparkSuccessor`，省掉一次系统调用。

把 `CANCELLED` 定成 `1` 而不是 `-4`，是个刻意的编码技巧：所有"还需要被唤醒"的状态都是负数（-1/-2/-3），所有"已经放弃"的状态是正数，于是全源码里只需要 `ws > 0` 和 `ws < 0` 两种比较，不用枚举。代价是可读性——第一次读的人很难从 `if (ws > 0)` 一眼看出这是在判断"节点已取消"，而不是"状态数值偏大"。

### 三、队列：为什么双向，为什么 head 是哨兵

AQS 的队列是 CLH 锁的变体。原始 CLH 是单向链表加自旋：每个线程在自己的前驱节点上 spin，前驱释放就自然观察到。AQS 做了两处改造——把自旋换成 `LockSupport.park`（避免空转），把单向换成双向。

改双向的直接原因是 `unparkSuccessor`。唤醒时需要"找到 head 之后第一个没取消的节点"，如果只有 `next`，一旦中间有节点取消就得靠遍历删除，而删除在无锁链表里非常难写。有了 `prev`，就可以从 `tail` 倒着走，跳过所有 `ws > 0` 的节点，一行循环解决。

更精妙的是入队的三个步骤：

```java
node.prev = pred;                    // ① 先挂 prev
if (compareAndSetTail(pred, node)) { // ② 再 CAS 抢 tail
    pred.next = node;                // ③ 最后连 next
    return node;
}
```

这三步不是原子的。假设线程 X 刚做完 ②、还没做 ③，此时另一个线程从 `head` 正向遍历，走不到 X——因为 `pred.next` 还是 null。但从 `tail` 反向遍历一定能走到 X，因为 `node.prev` 在 ② 之前就挂好了。**这就是 `prev` 可靠、`next` 不可靠的根源**，也是 `unparkSuccessor` 里那段"倒着找"的代码存在的原因，不是防御性编程，是必须。

`head` 是哨兵，不是持锁线程。这一点值得单独强调：`head.thread` 恒为 null，`head.prev` 恒为 null。它代表的是"上一个持锁者留下的位置"。真正的持锁线程根本不在队列里——它在 `tryAcquire` 成功那一刻就返回了，压根没入过队。所以严格说，队列是 `head.next` 开始的那一段，说"队列长度"时不该把 head 算进去。`setHead` 的语义也因此很清楚：我刚拿到锁，我把自己变成新的墓碑，旧墓碑可以回收了。

`enq` 里有两个细节常被误读。第一，`compareAndSetHead(new Node())` 之后紧跟的 `tail = head` 是普通写而不是 CAS，因为能进这个分支就说明刚才读到 `t == null`，而 head 刚被自己 CAS 成功，别的线程要么卡在同一个 CAS 上重试、要么还停在读 tail 为 null 的那一步，没人能同时写 tail。第二，这个分支**不 return**，只是把地基打好，必须回到循环顶部走 else 分支才真正把节点挂上去。很多人第一次读 `enq` 时会疑惑"设完 tail 怎么不返回"，答案就在这个 for 循环的形状里：初始化和入队是两轮循环，不是一轮。

还有一点值得对照：原始 CLH 里每个线程都在自己前驱的状态上自旋，天然不存在丢失唤醒的问题，因为自旋本身就是持续观察。AQS 把自旋换成 park，就必须引入 SIGNAL 这个显式协议——"我要睡了，你释放时叫我"——来替代持续观察。换句话说，`waitStatus` 里的 -1 不是设计上的优雅，而是阻塞的代价。

### 四、acquire 三步与一次完整推演

把上面所有零件装起来，走一遍两个线程的完整时序。先记住一个前提：`acquireQueued` 里 `p == head && tryAcquire(arg)` 这个条件，`p == head` 是必要条件。原因不是"只有队头才该拿锁"这种朴素的公平观，而是 head 是队列里唯一确定已经让出锁的节点——它曾经持锁，现在把位置腾出来了。其他位置的前驱可能还攥着锁，去试只是浪费一次 CAS。所以这个判断首先是性能优化，其次才顺带产生了顺序性。

**线程 A 调 `lock()`**：`tryAcquire` 里 `state == 0`，CAS `0→1` 成功，`setExclusiveOwnerThread(A)`，返回 true。`acquire` 的 `&&` 短路，整个方法结束。此刻 `head == tail == null`，队列根本没被创建。

**线程 B 调 `lock()`**：`tryAcquire` 失败（state 是 1 且 owner 不是 B）→ `addWaiter` 发现 `tail == null`，走 `enq`。`enq` 第一轮 CAS 建出哨兵 `new Node()`（ws=0，thread=null），`tail = head`，**注意这里不 return**，第二轮才把 B 挂上去。所以哪怕只有一个线程排队，队列里也是两个节点：一个空哨兵 + B。

**`acquireQueued(B)` 第一轮**：`p = head`，`p == head` 成立，再试一次 `tryAcquire`（万一 A 恰好在这几十纳秒里释放了呢），失败。进 `shouldParkAfterFailedAcquire(head, B)`：`head.ws == 0`，走 else 分支，CAS `0→-1`，**返回 false**。

**第二轮**：`p == head`，`tryAcquire` 仍失败；`shouldPark` 这次看到 `head.ws == -1`，返回 true；`parkAndCheckInterrupt` 里 `LockSupport.park(this)`，B 停住。

第一轮为什么不直接 park？因为从"决定要睡"到"真的睡下"之间有一个窗口，如果在这个窗口里 A 释放了锁并调用了 `unpark`，而 B 还没 park，就可能出现"叫早了"。`LockSupport` 的许可机制能兜住这个竞态（`unpark` 先于 `park` 会留下一个许可，后续 `park` 立即返回），但 Doug Lea 仍然选择先设 SIGNAL、再重试一次、确认抢不到才 park。这是一次典型的 double-check：把"可能已经轮到我"的情况在 park 之前消化掉，减少一次无意义的上下文切换。

**A 调 `unlock()`**：`tryRelease` 把 state 减到 0，owner 置 null，返回 true。`release` 里 `h = head`，`h != null && h.ws == -1` 成立，调 `unparkSuccessor(head)`：先把 `head.ws` CAS 成 0（唤醒责任已履行），`s = head.next = B`，`ws <= 0`，直接 `LockSupport.unpark(B.thread)`。

**B 醒来**：从 `parkAndCheckInterrupt` 返回 `Thread.interrupted()` = false，`interrupted` 保持 false，回到 for 顶部。`p = head`（还是那个哨兵），`tryAcquire` 这次成功，state `0→1`，owner = B。`setHead(B)`：`head = B`，`B.thread = null`，`B.prev = null`；`p.next = null` 断开旧哨兵。返回 false，`acquire` 结束，不触发 `selfInterrupt`。

此刻队列回到 `head == tail`，B 成了新哨兵，等下一个线程 C 从 `addWaiter` 的快路径挂上来。整个循环闭合。

最后补一个容易被跳过的收尾：`cancelAcquire`。它只在 `acquireQueued` 的 `finally` 里、且 `failed == true` 时被调用，典型场景是子类 `tryAcquire` 抛了异常（比如重入计数溢出）。做的事是：`node.thread = null` 断掉线程引用，`node.waitStatus = CANCELLED` 打上死亡标记，然后按位置分三种善后。自己是 tail，就把 tail 回退到前驱并断开 next；自己不是 tail 但前驱还活着，就把前驱的 next 直接指向自己的后继，让后面的人跳过自己；前驱是 head 或前驱也死了，就直接 `unparkSuccessor(node)` 把唤醒责任往下传。

关键认知是：取消的节点**不会立刻从链表上摘掉**，只是被标记。真正的物理摘除发生在别的线程的路径上——`shouldParkAfterFailedAcquire` 里那段 `do-while`，或者 `unparkSuccessor` 的反向遍历。这是无锁链表的典型做法：标记删除加惰性清理。如果在取消路径上直接做指针手术，就要处理"我正在摘自己，别人正在读我的 next"这种竞态，代价远高于让后来者顺手清理。

**容易误解的地方**：

- **以为 head 是当前持锁线程**。不是。`head.thread` 永远是 null。持锁线程在 `setHead` 之前根本不在队列里，在 `setHead` 之后又主动把自己的 thread 清空了。把 head 理解成"上一个持锁者的墓碑"比理解成"持锁者"准确得多。
- **以为 `waitStatus` 描述自己的状态、以为入队就立刻 park**。`SIGNAL` 描述的是后继，看到某个节点 ws=-1，正确解读是"它后面有人睡着，它释放时得负责叫醒"。正因为新节点自己的 ws 是 0，第一次进 `shouldParkAfterFailedAcquire` 必然返回 false——代码只是把前驱改成 -1 就退出了，真正 park 发生在第二轮循环。这个"多转一圈"是刻意设计的，不是低效。
- **以为 `unparkSuccessor` 直接取 `node.next`**。`next` 只是快路径。入队三步非原子，`next` 可能还是 null；节点取消后 `next` 也可能指向已取消的节点。所以 `s == null || s.waitStatus > 0` 时必须从 `tail` 反向遍历。这个分支在低并发下几乎不会走到，所以很多人读源码时直接跳过了。
- **以为 `acquire` 会响应中断**。不会。`parkAndCheckInterrupt` 里的 `Thread.interrupted()` 会**清除**中断标记，`acquireQueued` 只是把这件事记在 `interrupted` 变量里，一路返回到 `acquire`，最后用 `selfInterrupt()` 把标记补回去。所以 `lock()` 期间被中断，行为是"继续抢锁，抢到之后中断标记还在"，异常一个都不抛。想要可中断，得用 `lockInterruptibly()`，那条路径在 `parkAndCheckInterrupt` 返回 true 时直接 `throw new InterruptedException()`，`finally` 里 `cancelAcquire` 把节点标成 CANCELLED。

**记住这一句**：AQS 从不实现"锁"，它只实现"排队"；state 的含义和 tryAcquire 的判定，永远是子类的事。
