# 第4章 公平与非公平：差在那一行判断

**一句话**：两者只差两次 CAS 时机：非公平在入队前多抢一次，换来吞吐与饥饿。

**为什么需要它**：一个真实场景。线上有个任务调度线程持锁约 200ms 做批量计算，后面排着 12 个工作线程。监控显示：改成公平锁之后，P99 延迟反而从 210ms 涨到 380ms，吞吐掉了三成。团队里有人说"公平锁当然慢"，但说不出慢在哪。另一个场景正好相反：某线程在日志里连续出现 `park` → `unpark` → `park` 的循环，等了 3 秒还没拿到锁，而锁的持有时间只有 2ms——它被新来的线程反复插队，饿死了。

这两个现象来自同一行代码。`ReentrantLock` 的公平与非公平，实现上的差异只有两处，加起来不到十行，但它们决定了锁在"空闲的那几十微秒"里归谁。不理解这两处，就无法解释上面两种截然相反的表现，也无法在"吞吐"和"确定性"之间做有依据的取舍。

本章还要解决另一半问题：`Condition`。很多人把它当成 `wait/notify` 的语法糖，于是写出"signal 之后线程立刻继续执行"的错误假设。实际上 `await` 涉及**两条完全不同的队列**，一次 `signal` 只是一次搬家，不是一次唤醒。这两件事必须一起讲，因为 `ConditionObject` 的节点就是从同步队列那套 `Node` 复用的。

**核心结构**：

| 概念 | 作用 |
|---|---|
| `NonfairSync.lock()` | 上来先 CAS 抢一次，成功直接返回，连 `acquire` 都不进 |
| `FairSync.lock()` | 直接 `acquire(1)`，没有任何抢跑 |
| `nonfairTryAcquire` | 不看队列，state 为 0 就 CAS。`tryLock()` 也走它 |
| `hasQueuedPredecessors` | 公平锁的守门员：判断"我前面是否有人"，是则不许抢 |
| `ConditionObject.firstWaiter/lastWaiter` | 条件队列首尾，单向链表，与 `head/tail` 无关 |
| `Node.nextWaiter` | 条件队列指针，与 `next` 是两个独立维度 |
| `addConditionWaiter` | 包装成 ws=CONDITION 的节点，挂到条件队列尾 |
| `fullyRelease` | 一次性释放全部重入层数，返回 `savedState` |
| `isOnSyncQueue` | 判断节点是否已被搬到同步队列，是 `await` 自旋的退出条件 |
| `transferForSignal` | 把节点从条件队列搬到同步队列尾，必要时直接 unpark |
| `savedState` | 记住 await 前的重入次数，唤醒后原样恢复 |

**关键代码**：

```java
// 非公平：进门先抢，抢不到才走正常流程
static final class NonfairSync extends Sync {
    final void lock() {
        if (compareAndSetState(0, 1))              // 差异点一：不看队列，直接插队
            setExclusiveOwnerThread(Thread.currentThread());
        else
            acquire(1);                            // 失败了才和公平锁走同一条路
    }
    protected final boolean tryAcquire(int acquires) {
        return nonfairTryAcquire(acquires);        // 差异点二：这里也不看队列
    }
}

final boolean nonfairTryAcquire(int acquires) {
    final Thread current = Thread.currentThread();
    int c = getState();
    if (c == 0) {
        if (compareAndSetState(0, acquires)) {     // 没有 hasQueuedPredecessors
            setExclusiveOwnerThread(current);
            return true;
        }
    } else if (current == getExclusiveOwnerThread()) {
        setState(c + acquires);                    // 重入：这里不看队列，公平锁也不看
        return true;
    }
    return false;
}

// 公平：唯一的差别就是多了这个判断
protected final boolean tryAcquire(int acquires) {
    final Thread current = Thread.currentThread();
    int c = getState();
    if (c == 0) {
        if (!hasQueuedPredecessors() &&            // 差异点：前面有人就闭嘴排队
            compareAndSetState(0, acquires)) {
            setExclusiveOwnerThread(current);
            return true;
        }
    } else if (current == getExclusiveOwnerThread()) {
        setState(c + acquires);
        return true;
    }
    return false;
}
```

```java
public final void await() throws InterruptedException {
    if (Thread.interrupted()) throw new InterruptedException();
    Node node = addConditionWaiter();              // ① 进条件队列，ws = CONDITION(-2)
    int savedState = fullyRelease(node);           // ② 释放全部重入层数，state 归 0
    int interruptMode = 0;
    while (!isOnSyncQueue(node)) {                 // ③ 只要还在条件队列就睡
        LockSupport.park(this);                    //    被 signal 搬走后这里返回
        if ((interruptMode = checkInterruptWhileWaiting(node)) != 0) break;
    }
    if (acquireQueued(node, savedState) && interruptMode != THROW_IE)
        interruptMode = REINTERRUPT;               // ④ 在同步队列里重新抢锁，并恢复重入次数
    if (node.nextWaiter != null) unlinkCancelledWaiters();
    if (interruptMode != 0) reportInterruptAfterWait(interruptMode);
}

private void doSignal(Node first) {
    do {
        if ((firstWaiter = first.nextWaiter) == null) lastWaiter = null;
        first.nextWaiter = null;                   // 断开条件队列，防止重复转移
    } while (!transferForSignal(first) && (first = firstWaiter) != null);
}

final boolean transferForSignal(Node node) {
    if (!compareAndSetWaitStatus(node, Node.CONDITION, 0))
        return false;                              // CAS 失败说明节点已取消，跳过它
    Node p = enq(node);                            // 搬进同步队列尾，返回新前驱
    int ws = p.waitStatus;
    if (ws > 0 || !compareAndSetWaitStatus(p, ws, Node.SIGNAL))
        LockSupport.unpark(node.thread);           // 前驱死了或设不了 SIGNAL，只能当场叫醒
    return true;                                   // 正常情况：不 unpark，只是搬了个家
}
```

**拆解**：

### 一、两处差异，精确到行

差异一在 `lock()` 入口。非公平锁第一件事就是 `compareAndSetState(0, 1)`，这一步发生在任何排队逻辑之前，成功了连 `acquire` 都不进。公平锁没有这一步，直接 `acquire(1)`。

差异二在 `tryAcquire` 内部。两者都判断 `state == 0`，但公平锁多一个 `hasQueuedPredecessors()`：只要队列里有人排在我前面，即使锁此刻空闲，我也不许抢，乖乖去 `addWaiter`。非公平锁没有这个判断，看到 0 就 CAS。

`hasQueuedPredecessors` 的实现值得逐字读：

```java
public final boolean hasQueuedPredecessors() {
    Node t = tail;
    Node h = head;
    Node s;
    return h != t &&
        ((s = h.next) == null || s.thread != Thread.currentThread());
}
```

三种情况。`h == t` 表示队列空或只剩哨兵，返回 false，可以抢。`h != t` 但 `h.next == null`，说明有线程 CAS `tail` 成功了却还没写 `head.next`——也就是上一章说的入队三步非原子的那个窗口——此时**保守地返回 true**，让当前线程去排队。`h.next.thread == currentThread`，说明队头就是自己，返回 false。注意最后这个分支：公平锁判断的不是"队列非空"，而是"**我前面有没有人**"。队列里只有自己时，`hasQueuedPredecessors` 返回 false，照样可以抢。

重入路径两者完全一致，都不看队列。这一点很关键：公平锁的"公平"不覆盖重入，因为重入的是已经持锁的线程，它本来就不该排队。

还有个细节：非公平锁其实有**两次**插队机会。第一次在 `lock()` 入口那次 CAS，第二次在 `acquire → tryAcquire → nonfairTryAcquire`。这两次几乎在同一时刻发生，中间只隔了一次失败分支，所以第二次成功率不高，但它覆盖了一个真实场景：第一次 CAS 失败是因为锁正被占用，而在这几纳秒里持有者刚好释放了，第二次就能无缝接上，完全不需要入队。公平锁把这两次机会都关掉了——它连第一次 CAS 都不做，直接进 `acquire`，然后被 `hasQueuedPredecessors` 挡在门外。

### 二、为什么非公平吞吐更高

从释放锁到被唤醒的线程真正执行，中间有一条固定成本链：`unpark` → 线程进入就绪队列 → 调度器选中 → 上下文切换 → 从 `park` 返回 → 重新 `tryAcquire`。在主流服务器上，这条链是几十微秒量级。

这几十微秒里，`state` 是 0，锁是空闲的。非公平锁允许一个**正在 CPU 上运行**的新线程直接 CAS 拿走它——这个线程不需要被唤醒、不需要切换、不需要恢复上下文，它的栈是热的。相当于把锁传递的空窗期填满了。公平锁则严格禁止这种填充：即使锁空闲，只要队列里有人，新线程也必须入队，然后 park，然后等着被唤醒。锁从"释放"到"再次被持有"之间必然插入一次完整的上下文切换。

第二个原因是减少无效唤醒。非公平下，队列里的线程被唤醒后常常发现锁又被别人抢走了，只能重新 park——一次锁传递引发两次上下文切换，还多一次 `shouldParkAfterFailedAcquire` 的 CAS。公平锁减少了这种"唤醒即失败"的概率。

代价是饥饿。极端情况下，一个排在队头的线程可能连续被 N 个新来的线程插队，等待时间无上界。而且非公平锁的延迟抖动更大：快的时候几微秒，慢的时候可能几毫秒。

但要说清一个常见误解：**非公平不等于无序，更不等于"新线程总能赢"**。它只是"允许"插队，不"保证"插队。竞争激烈时新线程的 CAS 也大概率失败，最终还是要入队。真正被插队的窗口，只在锁刚释放、且新线程恰好在这个瞬间到达时才打开。反过来说，如果临界区很长、锁几乎从不空闲，那公平与非公平的实际表现差别很小——插队窗口根本不存在。

还有一笔隐性成本常被忽略：公平锁每次 `tryAcquire` 都要经 `hasQueuedPredecessors` 读 `head`、`tail`、`head.next` 三个 volatile 字段。高竞争下 `head` 被频繁改写（每次 `setHead` 都写一次），这条 cache line 会在多个核之间来回弹跳，读它的代价远高于一次普通读。非公平锁在 `lock()` 入口那次 CAS 只碰 `state` 一个字段，缓存亲和性好得多。所以公平锁多出来的开销不只是"多一次上下文切换"，还有"多读几个热点字段"。

实践上的取舍：默认用非公平（`new ReentrantLock()`），吞吐优先。只有在"必须防止饥饿"的场景才用公平锁——比如任务调度器、长事务、按序处理的流水线。而且要知道，公平锁保证的是"不插队"，不是"严格 FIFO 执行"：`tryLock()` 走的是 `nonfairTryAcquire`，即使在公平锁上也是插队的；重入也不排队。

回到开头那个"公平锁反而更慢"的现场：12 个工作线程每个都要走一遍完整的唤醒链，而锁传递是串行的，任何一次调度抖动都会累加到后面所有人身上，P99 自然被拉长。非公平锁让刚跑完上一个任务的线程直接接上，省掉的是这条串行链，不是省掉一次 CAS。这也说明一件事：公平锁的成本随队列长度线性增长，非公平锁的成本几乎与队列长度无关——后者才是它吞吐高的结构性原因。

### 三、Condition：第二条队列

`ConditionObject` 只有两个字段：`firstWaiter`、`lastWaiter`。它跟 AQS 的 `head/tail` 没有任何关系，是**完全独立的另一条队列**。

- **同步队列**：`prev` / `next` 双向，节点在等**锁**。
- **条件队列**：`nextWaiter` 单向，节点在等**条件**。

两条队列复用同一个 `Node` 类，但用的是两组不同的指针。所以一个节点可以同时"在同步队列的某个位置"和"曾经在条件队列的某个位置"，判断依据是 `waitStatus` 和 `prev`（`isOnSyncQueue` 就是这么判的）。

为什么要两条队列？因为"等条件"和"等锁"是两件事。生产者发现队列满了，它要等的是"队列不满"这个条件，而不是"锁被释放"。如果只有一条队列，`signal` 就退化成 `notify`——随机唤醒一个，被唤醒的线程醒来发现条件不成立，又得睡回去，这叫**惊群**。`Object.wait/notify` 只有一个等待集，所以多条件场景下只能用 `notifyAll` 全唤醒再各自检查，代价是 N-1 次无效唤醒。

`Condition` 把这件事做细了：一把锁可以 `newCondition()` 多次，每次生成一个独立的 `ConditionObject`，对应一条独立的条件队列。生产者等 `notFull`，消费者等 `notEmpty`，`signal` 精确唤醒对应的那一队队头。这是 `Condition` 相对 `wait/notify` 的核心优势，也是 `ArrayBlockingQueue` 用两个 `Condition` 而不是一个的原因。

条件队列的操作天然线程安全，不需要额外加锁——因为 `await` 和 `signal` 都必须在持锁状态下调用（`signal` 开头有 `isHeldExclusively()` 检查），同一时刻只有一个线程能碰这条队列。这也解释了为什么 `unlinkCancelledWaiters` 敢用毫无保护的三指针遍历写法：它是被锁保护着的，不需要任何 CAS。

另一个设计细节：`ConditionObject` 是 AQS 的**内部类**，不是 AQS 的子类。它要调 `enq`、`isOnSyncQueue`、`acquireQueued` 这些私有方法，做成内部类最省事。代价是强绑定了 AQS，所以 `Condition` 接口虽然独立，实现却只能长在 AQS 上——`StampedLock` 就因为没继承 AQS，只能自己实现一套条件等待，没法复用这个类。

### 四、await 与 signal 的转移过程

`await` 是五步：

**① `addConditionWaiter`**：`new Node(thread, Node.CONDITION)`，ws 直接是 -2，挂到 `nextWaiter` 链尾。入队前会检查 `lastWaiter.waitStatus != CONDITION`，如果是，说明队尾已取消，先调 `unlinkCancelledWaiters` 清理一遍。

**② `fullyRelease`**：`savedState = getState()`，然后 `release(savedState)` 一次性把 state 减到 0。注意参数是 `savedState` 而不是 1——如果之前重入了 3 层，这里必须一次放干净，否则别的线程根本拿不到锁，而自己已经睡了，死锁。释放失败（当前线程没持锁）就抛 `IllegalMonitorStateException`，`finally` 里把节点标成 CANCELLED，留给后面的人清理。

**③ 自旋 park**：`while (!isOnSyncQueue(node)) park(this)`。退出只有两个口子：节点被 `signal` 搬到了同步队列，或者线程被中断（`checkInterruptWhileWaiting` 返回非 0）。`isOnSyncQueue` 的判断顺序有讲究：`ws == CONDITION` 说明还没被搬；`prev == null` 说明没进过同步队列（`prev` 只在 `enq` 里设置）；`next != null` 说明肯定在队列里；三者都不满足时，从 `tail` 反向查一遍——还是因为入队三步非原子，`prev` 有了但 `next` 还没有的那个瞬间。

**④ `acquireQueued(node, savedState)`**：此时节点已经在同步队列里，走的是和普通抢锁完全一样的逻辑。参数传 `savedState` 而不是 1，是为了恢复重入层数：await 前 state 是 3，唤醒后要重新变成 3，否则 `unlock` 次数就对不上了。

**⑤ 处理中断**：`THROW_IE`（signal 之前中断，抛 `InterruptedException`）或 `REINTERRUPT`（signal 之后中断，补标记不抛）。区分依据是 `transferAfterCancelledWait` 里那次 `CAS(CONDITION, 0)` 成不成功——成功说明 signal 还没发生，中断赢了；失败说明 signal 已经把 ws 改成 0 了，中断来晚了。

`signal` 只有一步实质动作：`transferForSignal`。CAS 把 `ws` 从 -2 改成 0（失败说明节点已取消，直接跳过，处理下一个），`enq` 搬进同步队列尾，然后看新前驱：如果前驱 `ws > 0`（已取消）或者 CAS 成 SIGNAL 失败，就当场 `unpark`；否则什么都不做。

**正常路径下 signal 不唤醒任何线程**。它只是把节点从一条队列搬到另一条队列。被搬的线程还在 `park`，它要等同步队列里排在它前面的人都走完、前驱释放锁时才会被 `unpark`。这一点是理解 `Condition` 的分水岭。

走一遍完整时序，把两条队列串起来：

- A 持锁（state=1），B 调 `await()`：B 进条件队列（ws=-2），`fullyRelease` 把 state 减到 0、owner 置 null，B park。
- C 也调 `await()`：条件队列变成 `B → C`，state 仍是 0。
- D 调 `lock()`：拿到锁，state=1，同步队列里只有哨兵。
- A（此时 A 已经释放了锁，但它仍持有 `Condition` 的引用，通常由持锁的 D 来调）调 `signal()`：B 被 CAS 成 ws=0，`enq` 到同步队列尾。**B 仍在 park**。
- D 调 `unlock()`：`unparkSuccessor` 唤醒 B。B 从 `await` 里的 park 返回，`isOnSyncQueue` 为 true，跳出循环，`acquireQueued(B, savedState=1)` 拿到锁，state 恢复成 1，`await` 返回。
- 此时 C 还在条件队列里，等下一次 `signal`。

关键在于倒数第二步：B 从"被 signal"到"从 await 返回"，中间隔着一次完整的抢锁。如果同步队列里本来就有人排队，B 还得排在它们后面。**所以 `await` 返回不代表条件一定成立**——这就是 `BoundedBuffer` 里必须写 `while (count == items.length) notFull.await();` 而不能写 `if` 的原因。被唤醒只是"条件可能变了"，不是"条件一定满足了"。虚假唤醒（spurious wakeup）也会让 `if` 出错，`while` 是唯一正确的写法。

顺带说下带超时的版本。`awaitNanos` 的结构和不带超时的几乎一样，差别在循环体内：剩余时间小于 `spinForTimeoutThreshold`（1000 纳秒，即 1 微秒）时不再调 `parkNanos`，改成纯自旋。原因是 `parkNanos` 要进内核、要挂定时器、要处理时钟精度，开销远大于自旋这 1 微秒。超时后调 `transferAfterCancelledWait` 自己把节点搬进同步队列——**即使超时，节点也必须进同步队列**，否则 `acquireQueued` 拿不到合法的前驱，会在 `node.predecessor()` 上直接 NPE。这个方法的返回值还顺带回答了"到底是超时还是 signal 抢先"：返回 true 是超时（CAS `CONDITION→0` 成功，说明 signal 还没动过），返回 false 是 signal 已经搬过了。

**容易误解的地方**：

- **以为公平锁就是严格先来后到、以为 `hasQueuedPredecessors` 在判断队列非空**。它判断的是"我前面有没有人"：队列里只有自己时返回 false，照样可以抢。公平也只保证"队列里有人时不插队"，不保证 FIFO 执行顺序——`tryLock()` 在公平锁上走的仍是 `nonfairTryAcquire`，照样插队；重入路径也不看队列；被唤醒的线程和刚到的线程在 `tryAcquire` 上的竞争本身就没有顺序保证。
- **以为 `await` 只释放一层锁**。`fullyRelease` 传的是 `getState()`，一次放干净。如果只减 1，重入 3 层的线程睡了之后 state 还剩 2，别的线程永远拿不到锁，直接死锁。唤醒后靠 `savedState` 恢复层数，这一对是配套的。
- **以为 `signal` 会让线程立刻继续执行**。`signal` 只是把节点从条件队列搬到同步队列，不改变 park 状态（除非前驱已取消）。线程真正恢复执行要等它抢到锁。把 `signal` 理解成"搬家"而不是"唤醒"，后面所有行为都能推出来。
- **以为一把锁只有一个等待队列**。每个 `Condition` 一条。`signal` 精确唤醒指定条件队列的队头，这是它比 `notify`（一个 monitor 一个等待集，只能随机唤醒）强的地方。

**记住这一句**：公平与非公平的差别不在队列，而在"进队列之前允不允许再试一次"；Condition 的差别不在锁，而在多了一条只等条件的队列。
