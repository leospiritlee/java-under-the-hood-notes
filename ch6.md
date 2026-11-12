# 第6章 读写锁：读读不互斥是怎么做到的

**一句话**：把一个 int 劈成两半，读锁总数在高 16 位、写锁重入在低 16 位，一次 CAS 同时判定两者。

**为什么需要它**：一个本地缓存，读远多于写。用 ReentrantLock 保护，10 个读线程被串行化，吞吐掉到单线程水平。直觉方案是上两把锁：读的时候抢读锁，写的时候抢写锁。这个方案有个致命窗口——

读线程检查"写锁没被占用"，通过；此刻写线程检查"读锁计数为 0"，也通过；两边各自 CAS 自己那把锁，都成功。结果是写线程在改数据的同时，读线程正在读。读写互斥被打破，而且这种 bug 只在极窄的时间窗出现，压测压不出来，上线靠运气。

根因是：读和写的判定必须原子。两个独立的 state 变量、两次独立的 CAS，中间必然有缝。要么引入一把元锁把两次判定串起来（那读读也互斥了，回到原点），要么把两个计数塞进同一个变量，让一次 CAS 完成两次判定。

ReentrantReadWriteLock 选了后者：state 是 32 位 int，高 16 位记读锁总持有数，低 16 位记写锁重入次数。这一章讲的就是这个位布局如何支撑"读读共享、读写互斥、写独占"，以及它带来的三个副作用：65535 硬上限、读锁重入必须靠 ThreadLocal 记、升级不可行而降级可行。

**核心结构**：

| 概念 | 作用 |
|---|---|
| `SHARED_SHIFT = 16` | 分界线。读锁计数在 bit[16,31]，写锁在 bit[0,15] |
| `SHARED_UNIT = 1 << 16` | 读锁 +1 就是 state += 65536，低位一位不动 |
| `MAX_COUNT = 65535` | 单个 16 位段的上限 |
| `sharedCount(c) = c >>> 16` | 无符号右移取高 16 位 |
| `exclusiveCount(c) = c & 0xFFFF` | 掩码取低 16 位 |
| `HoldCounter` | 每线程的读锁重入次数 + 线程 id |
| `readHolds` | `ThreadLocal<HoldCounter>`，重入计数的权威来源 |
| `cachedHoldCounter` | 缓存"最后一个获取读锁的线程"的计数器，省一次 ThreadLocal 查找 |
| `firstReader` / `firstReaderHoldCount` | 把读计数从 0 变 1 的线程及其层数，读无竞争时的最快路径 |
| `readerShouldBlock()` | 读锁是否该退让，公平/非公平两套实现 |
| `writerShouldBlock()` | 写锁是否该退让，非公平模式恒为 false |

**关键代码**：

```java
// 读锁获取：一次 CAS 只动高 16 位
protected final int tryAcquireShared(int unused) {
    Thread current = Thread.currentThread();
    int c = getState();
    // 有写锁且持有者不是自己 → 失败。这个"不是自己"的豁免，就是锁降级的入口
    if (exclusiveCount(c) != 0 && getExclusiveOwnerThread() != current)
        return -1;
    int r = sharedCount(c);
    if (!readerShouldBlock() && r < MAX_COUNT &&
        compareAndSetState(c, c + SHARED_UNIT)) {
        if (r == 0) {                        // 我是把读计数从 0 变 1 的人
            firstReader = current;
            firstReaderHoldCount = 1;
        } else if (firstReader == current) {
            firstReaderHoldCount++;          // 最快路径：一次自增，不碰 ThreadLocal
        } else {
            HoldCounter rh = cachedHoldCounter;
            if (rh == null || rh.tid != getThreadId(current))
                cachedHoldCounter = rh = readHolds.get();
            else if (rh.count == 0)
                readHolds.set(rh);           // 缓存命中但 count 为 0，说明 Entry 被 remove 过
            rh.count++;
        }
        return 1;
    }
    return fullTryAcquireShared(current);    // CAS 失败或有队列，还有一次机会
}
```

```java
// 写锁获取：只要有读锁在，就失败
protected final boolean tryAcquire(int acquires) {
    Thread current = Thread.currentThread();
    int c = getState();
    int w = exclusiveCount(c);
    if (c != 0) {
        // c != 0 而 w == 0：state 里只有读锁（可能还是自己持有的）
        // c != 0 且 w != 0 但持有者不是自己：写锁被别人拿着
        if (w == 0 || current != getExclusiveOwnerThread())
            return false;
        if (w + exclusiveCount(acquires) > MAX_COUNT)
            throw new Error("Maximum lock count exceeded");
        setState(c + acquires);      // 能走到这里是写锁重入，独占，不需要 CAS
        return true;
    }
    if (writerShouldBlock() || !compareAndSetState(c, c + acquires))
        return false;
    setExclusiveOwnerThread(current);
    return true;
}
```

**拆解**：

### 位运算：为什么一次 CAS 能完成两次判定

读锁 +1 是 `state += 65536`，写锁 +1 是 `state += 1`。两者在二进制上不重叠，所以可以独立累加而不互相覆盖——前提是 CAS 的期望值取对了。判断"能不能拿读锁"看 `exclusiveCount(c) != 0`，判断"能不能拿写锁"看 `exclusiveCount(c) == 0 && c == 0`，两个判断读的都是同一个 c，同一次 CAS 提交，中间没有缝。这就是整个设计的全部价值。

`sharedCount(c) = c >>> 16` 必须用无符号右移。如果读锁总数超过 32767，第 31 位变成 1，用 `>>` 会得到负数，`r < MAX_COUNT` 判断直接失效，读锁逻辑崩掉。JDK 这里写的是 `>>>`，一个字符的差别就是三万并发读下的一个 bug。

`exclusiveCount(c) = c & 0xFFFF` 是掩码，天然只取低位，不怕符号位。

为什么两边各 16 位？写锁要能表达重入层数，8 位只有 255 层太容易撞；读锁要能容纳高并发读，16 位给到 65535。对称取 16 是折中。代价是硬上限：单线程写锁重入超过 65535 抛 `Error`，全部线程读锁总数超过 65535 也抛 `Error`。注意读锁的 65535 是全局总和，不是每线程——1000 个线程各重入 66 层就会撞线。虽然罕见，但这是真实事故来源。

### 读锁重入：为什么非得引入 ThreadLocal

state 的高 16 位是全局读锁总数。unlock 时有一个必须回答的问题：这次释放是不是当前线程的最后一层？如果无脑减 65536，一个线程 unlock 两次就会把别人的额度也减掉，state 漂移，最终该唤醒写者时不唤醒，或者提前唤醒。

所以每个线程需要自己的计数器。但绝大多数读锁是无竞争的，每次都走 `ThreadLocal.get()` 太贵（ThreadLocalMap 开放寻址、要算 hash、要处理 Entry 状态）。于是有了三级结构，从快到慢：

1. `firstReader` + `firstReaderHoldCount`：把读计数从 0 变 1 的那个线程。它的层数直接放在一个普通 int 字段里，重入就是 `++`，释放就是 `--`，完全不碰 ThreadLocal。名字有误导性——它不是"第一个线程"，而是"当前唯一持有读锁的那个线程"；它释放到 0 后 `firstReader` 置 null，下一个把计数从 0 变 1 的线程接手。
2. `cachedHoldCounter`：缓存最后一个成功获取读锁的线程的 HoldCounter。多读线程交替时，unlock 的线程往往就是上一次 lock 的线程，命中率高，省掉一次 ThreadLocal 查找。用 tid 校验，不匹配就退回 ThreadLocal。
3. `readHolds`：ThreadLocal，权威来源。释放到 0 时 `readHolds.remove()`，防止线程池里的线程长期挂着一个 HoldCounter（值不大，但 Entry 会一直留在 ThreadLocalMap 上）。

这三层是纯优化，语义等价于"一个 `ThreadLocal<HoldCounter>`"。理解这一点，读锁那 100 行就不难了。`fullTryAcquireShared` 里那段"`get()` 之后发现 count == 0 就 `remove()`"的代码，也是为了让一次失败的尝试不留下垃圾 Entry。

释放侧的收尾同样值得看一眼。`tryReleaseShared` 先把当前线程的计数减 1（firstReader 走字段、其他线程走 HoldCounter，减到 0 就 `remove()` 防泄漏），然后在 for 循环里 `nextc = c - SHARED_UNIT`，CAS 成功后 `return nextc == 0`。注意这个返回值不是"释放成功"，而是"整个 state 归零了"——高低 16 位全为 0，意味着读锁和写锁都空了。只有这一种情况才需要 `doReleaseShared`，因为队列里排着的多半是等写锁的节点：只要还剩一个读者，写者就不该被唤醒，唤醒了也会因为 `w == 0 || owner != current` 而重新 park，纯属浪费一次上下文切换。写锁释放用的是同一个思路，`tryRelease` 里 `boolean free = exclusiveCount(nextc) == 0`，只有重入层数归零才算真释放。两个模式的"释放完成"判定，都归结为"state 里对应的那段是否清零"。

还有一个细节值得单独说：`Sync` 的构造方法里有 `setState(getState())`。看着像废话，实际是写屏障——`readHolds` 是普通字段，靠这次 state 的 volatile 写，保证其他线程看到这个 Sync 时，`readHolds` 已经初始化完成。

### readerShouldBlock：非公平模式并不是无脑抢

读锁获取的第一道门是 `!readerShouldBlock()`。两套实现差别很大：

- FairSync：`hasQueuedPredecessors()`。队列里有人排队就退让，标准公平语义。
- NonfairSync：`apparentlyFirstQueuedIsExclusive()`。只有当 head.next 是写者时才退让。

为什么非公平模式要给写者特殊照顾？因为读锁是共享的，读线程可以源源不断插队。完全不设限的话，一个读请求持续不断的系统里写线程永远排不到——写饥饿。判 head.next 是不是写者，是一个极低成本的方向性保护：写者已经排到队头了，新来的读线程让一让；写者不在队头，那大家就比 CAS 速度。

但检查只发生一次。CAS 失败或者被 shouldBlock 拦下，会进 `fullTryAcquireShared` 再试。这个方法里最关键的不是重试，而是那个 `else if (readerShouldBlock())` 分支：如果 `firstReader == current`，直接放行；否则去查 HoldCounter，`count > 0` 说明是重入，放行，`count == 0` 才 `return -1` 去排队。

一个已经持有读锁的线程再次 lock 读锁（重入）。如果让它去排队，而队头是等写锁的写者，写者在等所有读锁释放，这个线程在等写者让路——自己等自己，死锁。所以重入必须无条件成功，哪怕队列里有人。这是 `fullTryAcquireShared` 存在的第二个理由（第一个是给 CAS 失败一次重试机会）。

判断"是不是重入"的办法就是查 HoldCounter：`firstReader == current`，或者 ThreadLocal 里 `count > 0`。

### 降级可行、升级不可行

降级：持有写锁 → 获取读锁 → 释放写锁。看读锁 `tryAcquireShared` 的第一行，`exclusiveCount(c) != 0 && getExclusiveOwnerThread() != current`。写锁被自己持有时这个条件为 false，直接往下走，CAS 把高 16 位加 1，成功。降级在源码层面就是被显式允许的。

升级：持有读锁 → 获取写锁。看写锁 `tryAcquire` 的 `if (c != 0)` 分支：`w == 0` 直接 `return false`。此时 c 的高位全是读锁（包括自己那一份），低位是 0，所以 w == 0，失败，然后 `acquireQueued` 入队 park。而唤醒它的条件是"所有读锁释放"，包括它自己手里那把——它永远不会释放，因为它在 park。自锁，而且是那种日志里看不出所以然的自锁：线程栈停在 `park`，没人持锁，但就是不动。

降级的真正意义不是"提升性能"，是消除窗口。回到那个缓存例子：写完数据后如果先 unlock 写锁再 lock 读锁，中间这个空隙里另一个写线程可以抢到写锁改掉数据，你接着读到的就不是你刚写的。降级保证从写锁到读锁的交接期间没有其他写者能插入。注意它不是原子的——读锁的获取发生在写锁释放之前，只是顺序被固定了。所以写法必须是 try 块里 lock 读锁、finally 里 unlock 写锁，顺序反了就等于没降级。

StampedLock 提一句：JDK 8 引入，用版本号（stamp）代替读锁计数。`tryOptimisticRead` 读的时候完全不加锁、只取一个 stamp，读完 `validate` 一次，没被写者改过就直接用，改过再退化到悲观读。代价是不可重入、不支持 Condition、乐观读不阻塞写者（写者可能一直插队）。读多写极少的场景吞吐能高一个量级，代码复杂度也上一个量级。

**容易误解的地方**：

1. "读读不互斥"不等于"读锁不排队"。有写者持有写锁时，读锁一样要等；非公平模式下如果队头是写者，新来的读锁也要让。读读共享只在没有写者、或写者不在队头时成立。

2. `cachedHoldCounter` 是普通字段，被多线程并发读写，看着像数据竞争。它确实会读到过期值，但有 tid 校验兜底，读错了退回 ThreadLocal，正确性不受影响。它靠 state 的 volatile 语义间接获得可见性，本身不是同步点。

3. 65535 是全局读锁总数上限，不是每线程重入上限。写锁的 65535 才是单线程重入上限。两者共用同一个 `MAX_COUNT` 常量，容易混淆。

4. 读锁不能创建 Condition。`readLock().newCondition()` 直接抛 `UnsupportedOperationException`，因为 Condition 的等待队列要求独占语义——多个读线程同时 await，`signal` 该唤醒谁无法定义。

5. 降级不是"必须"的。如果读操作不依赖刚写入的数据（比如只打个日志），先放写锁再拿读锁也没问题，代价是多一次加锁和一个窗口。

**记住这一句**：读写锁的全部精巧都在那个 int 里——把两个互相排斥的判断压进一次 CAS，剩下的代码都是为这次 CAS 服务。
