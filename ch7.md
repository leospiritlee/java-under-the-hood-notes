# 第7章 Phaser：比 CountDownLatch 更灵活的分阶段同步

**一句话**：Phaser 把"参与者数量"和"阶段编号"都做成可变状态，一个实例能跑完任意多个阶段。

**为什么需要它**：一个数据导入任务分三阶段——解析、校验、入库，每阶段用一批工作线程，线程数按数据量动态决定。第一版用 `CyclicBarrier(10)`：

- 某天数据量小，只起了 8 个线程。8 个线程全部 await，barrier 的 count 停在 2，永远不归零，8 个线程全挂死，主线程在 `future.get()` 上等到超时。
- 你想改成"起线程之前先算好数量再 new"，但线程池是共享的，实际并发度受池大小限制，算不准。
- 三阶段要三个 CyclicBarrier，每阶段结束还要手工把结果传给下一阶段。
- 某个线程抛异常，`breakBarrier` 把整代打破，其他线程收到 `BrokenBarrierException`；你想 `reset()` 重来，而 reset 会让已经在等的线程全部异常返回。

换 CountDownLatch 好一点（它不要求参与者互相等待），但 latch 一次性：三个阶段三个 latch，每个都要单独传引用、单独 await，"谁负责在最后一个 countDown 之后做收尾"还得调用方自己判断。

这三个工具的差别不是 API 风格，是模型。CyclicBarrier 假设"参与者集合固定、每代重来"，CountDownLatch 假设"一次性事件"。真实的分阶段任务里，参与者会中途加入、中途退出，阶段数在运行时才知道。

Phaser 就是为这个模型设计的：parties 动态增删，phase 自动递增，一个实例贯穿全流程，还带每阶段回调。

**核心结构**：

| 概念 | 作用 |
|---|---|
| `state`（long） | 四段位域：bit[0,15] 未到达数、bit[16,31] parties、bit[32,62] phase、bit63 terminated |
| `phase` | 当前阶段号，从 0 递增，到 `Integer.MAX_VALUE` 后回绕到 0 |
| `parties` | 已注册的参与名额数。`register`/`bulkRegister` 加，deregister 减 |
| `unarrived` | 本阶段还没到达的名额数，每 arrive 一次减 1，减到 0 触发推进 |
| `register()` | parties + 1，同时让**当前阶段**的 unarrived + 1（新人也要等本阶段） |
| `arrive()` | unarrived - 1，不阻塞；若是最后一个，当场推进阶段并唤醒等待者 |
| `arriveAndDeregister()` | arrive + parties - 1，用于中途退出 |
| `arriveAndAwaitAdvance()` | arrive，然后等本阶段结束 |
| `awaitAdvance(phase)` | 等指定阶段结束，参数是调用者自己记录的阶段号 |
| `onAdvance(phase, parties)` | 阶段推进时的回调，返回 true 则终止 Phaser |
| `QNode` / `evenQ` / `oddQ` | 等待者节点，按阶段奇偶分两个 Treiber 栈顶 |

**关键代码**：

```java
// 位域划分
private static final int  UNARRIVED_MASK = 0xffff;              // bit 0-15
private static final long PARTIES_MASK   = 0xffff0000L;         // bit 16-31
private static final long TERMINATE_BIT  = 0x8000000000000000L; // bit 63
private static final int  PARTIES_SHIFT  = 16;
private static final int  PHASE_SHIFT    = 32;

private int doArrive(int adjust) {
    final Phaser root = this.root;
    for (;;) {
        long s = (root == this) ? state : reconcileState();  // 子节点先跟 root 对齐
        int phase = (int)(s >>> PHASE_SHIFT);
        if (phase < 0)
            return phase;                                     // 已终止，返回负数
        int unarrived = ((int)s == EMPTY) ? 0 : ((int)s & UNARRIVED_MASK);
        if (unarrived <= 0)
            throw new IllegalStateException(badArrive(s));
        if (UNSAFE.compareAndSwapLong(this, stateOffset, s, s -= adjust)) {
            if (unarrived == 1) {                             // 我是最后一个到达的
                long n = s & PARTIES_MASK;                    // 保留 parties 段
                int nextUnarrived = (int)n >>> PARTIES_SHIFT;
                if (root == this) {
                    if (onAdvance(phase, nextUnarrived))       // 回调在最后一个到达者的线程上跑
                        n |= TERMINATE_BIT;
                    else if (nextUnarrived == 0)
                        n |= EMPTY;                           // parties 归零，标记空
                    else
                        n |= ((long)phase + 1) << PHASE_SHIFT; // phase + 1，unarrived 段自动等于 parties
                    UNSAFE.compareAndSwapLong(this, stateOffset, s, n);
                    releaseWaiters(phase);                    // 唤醒本阶段的等待者
                } else if (nextUnarrived == 0) {
                    phase = parent.doArrive(ONE_DEREGISTER);  // 子节点空了，向父注销
                    UNSAFE.compareAndSwapLong(this, stateOffset, s, s | EMPTY);
                } else {
                    phase = parent.doArrive(ONE_ARRIVAL);
                }
            }
            return phase;
        }
    }
}
```

```java
// 等待者用 Treiber 栈存，按 phase 奇偶分两个栈顶
static final class QNode {
    final Phaser phaser;
    final int phase;
    final boolean interruptible;
    final boolean timed;
    volatile boolean wasInterrupted;
    volatile Thread thread;
    volatile QNode next;        // LIFO 指针
}

private void releaseWaiters(int phase) {
    QNode q; Thread t;
    AtomicReference<QNode> head = (phase & 1) == 0 ? evenQ : oddQ;
    // 栈顶节点的 phase 还没追上当前 phase，就说明它是上一轮遗留，摘下来唤醒
    while ((q = head.get()) != null &&
           q.phase != (int)(root.state >>> PHASE_SHIFT)) {
        if (head.compareAndSet(q, q.next) && (t = q.thread) != null) {
            q.thread = null;
            LockSupport.unpark(t);
        }
    }
}
```

**拆解**：

### 一个 long 装四个字段，arrive 就是一次减法

位域这么切的原因：parties 和 unarrived 各 16 位，所以单实例最多 65535 个参与者，超了只能上分层；phase 用 31 位，够 20 亿次推进；terminated 单独占最高位，因为它必须在任何一次读 state 时被立刻看到，不能藏在别的字段里。

arrive 的核心是一次 CAS：`s - adjust`。unarrived 在最低 16 位，只要它大于 0（前面已经检查），减 1 不会向高位借位。这是位域设计的红利——不用拆字段、不用拼回去，一个减法就完成了"到达"。

但减完之后要判断是不是最后一个，这就是 `if (unarrived == 1)` 分支：CAS 之后 unarrived 段已经是 0，需要重新构造一个 state。做法是 `n = s & PARTIES_MASK` 只保留 parties 段，然后 `n |= (phase + 1) << 32`。注意此时 unarrived 段是 0，也就是新阶段"一个人都还没到"——但新阶段需要等的人数是 parties，所以这里其实还差一步：源码靠 `nextUnarrived` 判断是否为空，实际的 unarrived 重置由后续 `n` 的构造隐式完成（新 state 的 unarrived 段置 0，等价于"新阶段所有人未到"，与 parties 相同）。这个技巧很省事，也很容易看漏。

`onAdvance` 在最后一个 arrive 的线程上同步执行，位置是 CAS 之后、`releaseWaiters` 之前。两个后果：

- 如果 onAdvance 抛异常，阶段已经推进了，但等待者没被唤醒。这批线程会在下一次有人 arrive 时被动释放，表现为"偶发卡顿"。
- 如果 onAdvance 返回 true，state 被打上 TERMINATE_BIT，之后所有 arrive/await 直接返回负数。

所以 onAdvance 里只能放轻量、且不应该抛异常的逻辑。

同一套状态还能表达三种不同的同步语义，这是 Phaser 最被低估的地方：

- 只 `arrive()` 不 await，等于 CountDownLatch——到达即走，谁在等由调用方决定。
- `arriveAndAwaitAdvance()`，等于 CyclicBarrier——到达并互相等待，而且自动开启下一轮。
- 配合 `awaitAdvance(phase)` 单独使用，等于"阶段监听器"——不参与计数，只在某个阶段结束时被放行。

三种用法的差别不在 API，而在"到达之后走不走"。这也是它比 CyclicBarrier 更适合不定数量参与者的根本原因：CyclicBarrier 的 `parties` 是 `final` 字段，构造时定死，`count` 每代从 `parties` 重置，参与者集合在语义上被冻结；Phaser 的 parties 就活在 state 的 16 位里，register 一次加一，deregister 一次减一，随阶段演进。所以"这一批 8 个线程、下一批 12 个"在 Phaser 里只是多调几次 register，在 CyclicBarrier 里只能重建实例——而重建意味着已经在等的线程全部作废。

### phase 与 awaitAdvance：为什么参数要传旧 phase

`arriveAndAwaitAdvance` 的实现就是两行：`int phase = arrive();` 拿到自己到达时的阶段号，再 `return awaitAdvance(phase);`。

`arrive` 返回的是"我到达时所在的阶段号"。`awaitAdvance(phase)` 的逻辑是：如果当前阶段已经不等于这个 phase（说明我到达之后，这班车已经开走了），立刻返回；否则入栈 park。

为什么不让 `awaitAdvance` 自己去读当前 phase？因为 arrive 和 awaitAdvance 之间有窗口：arrive 之后可能瞬间就有别的线程完成最后一次 arrive，阶段已经推进。此时 `awaitAdvance` 读到的是新 phase，如果拿新 phase 去等，就会白等一整轮。

传旧 phase 的语义是"我要等的是我参与的那一班"，开走了就直接走。这是 Phaser 里最容易写错的地方：手写 `awaitAdvance(phaser.getPhase())` 几乎总是错的，因为 `getPhase()` 和 `awaitAdvance` 之间还有窗口。正确做法是在同一个线程里先把 phase 存下来再传。

返回值是"实际到达的阶段号"，正常情况下等于 phase + 1，终止时返回负数。用返回值可以判断有没有被终止。

### Treiber 栈：为什么等待者用栈而不是队列

AQS 用 CLH 队列，因为锁有先后顺序，必须 FIFO。Phaser 不需要顺序：同一个 phase 上的等待者完全等价，阶段推进时全体放行，谁先被 unpark 无所谓。

既然无序，栈就是最优选：只要一个 head 指针，push/pop 都是一次 CAS，没有 tail、没有双向指针、没有前驱的 waitStatus 要维护。这是标准的 Treiber 栈——无锁 LIFO。

但 Phaser 的栈有个特殊设计：两个栈顶，`evenQ` 和 `oddQ`，按 phase 的奇偶选。原因是等待者的 phase 可能和当前 phase 不一致：

线程 A 在 phase 5 入栈等待。阶段推进到 6，A 被唤醒但还没来得及出栈（被中断、或 unpark 后还没被调度）。此时线程 B 在 phase 6 调 `awaitAdvance(6)` 入栈。如果只有一个栈，B 会被 push 到栈顶，而 `releaseWaiters(5)` 正在从栈顶往下摘——它会摘到 B。B 醒来发现 phase 还是 6，重新 park，白醒一次；更糟的是唤醒循环的条件依赖节点 phase，混在一起会让"该唤醒的没唤醒"。

用奇偶分栈，phase 5 的节点在 oddQ，phase 6 的在 evenQ，物理隔离。`releaseWaiters(5)` 只碰 oddQ，摘到 phase 已经追上来的节点就停，不会误伤。

pop 用 `head.compareAndSet(q, q.next)`：先读栈顶 q，再把栈顶换成 q.next。CAS 失败（有人同时 push 或 pop）就重新读 q 重试。这是无锁栈的标准写法。注意它不处理 ABA——这里 ABA 无害，因为节点一旦出栈就不会再入栈，指针不会回到旧值。

顺便回答一个常见疑问：Phaser 为什么不直接复用 AQS？两个原因。一是容量，AQS 的 state 是 int，装不下 parties、unarrived、phase、terminated 四段信息，硬塞就得再加一个字段，那 CAS 的原子性就没了。二是语义，AQS 的 CLH 队列是 FIFO 且每个节点挂着 waitStatus 由后继维护，这套结构是为"有先后顺序的独占获取"设计的；而 barrier 的等待者完全无序，维护双向链和 waitStatus 纯属浪费。Phaser 干脆自己写了一个只做"批量唤醒"的无锁栈，代码量比复用 AQS 还少。这也是 JDK 里少见的"不基于 AQS 的同步器"，和 CyclicBarrier 用 Condition 拼出来是同一个思路：工具要匹配问题形状，不要为了统一而统一。

### 分层：当 65535 不够，或者 CAS 太挤

单个 Phaser 的所有状态在一个 long 里，所有 arrive 都在 CAS 这一个变量。parties 到几千时 CAS 失败率急剧上升，自旋成本吃掉同步收益。

Tiering 的做法是把多个 Phaser 组成一棵树：叶子各自管一小批参与者，内部 CAS 只在自己的 state 上；子节点作为一个整体，向父节点注册一个 party。

几条必须记住的规则：

- `new Phaser(parent)` 不会立刻在父节点注册，要等子节点的第一个 party 注册时才注册。
- 整棵树的 phase 由 root 统一控制。子节点本地所有人都到了之后，还要等 root 推进，因为 root 要等所有子节点。
- 子节点的 `getPhase()` 要和 root 对齐，源码里那一堆 `reconcileState()` 就是在做这件事。
- 树结构下，子节点 parties 归零不会让自己 terminated，终止由 root 决定。

实际项目里几乎用不到分层，除非 parties 稳定在几千以上。知道它存在、知道它解决什么问题，就够了。

**容易误解的地方**：

1. `register()` 注册的是"一个参与名额"，不是"一个线程"。同一个线程可以 register 多次、arrive 多次，Phaser 不关心调用者是谁。反过来说，一个线程 register 两次却只 arrive 一次，整个阶段就卡住。

2. `arrive()` 不阻塞，但它可能触发阶段推进。如果它是最后一个到达的，`onAdvance` 会在当前线程上同步执行，然后唤醒所有等待者。所以 arrive 也可能很慢。

3. 中途 register 会拖住当前阶段。register 让 parties +1 且 unarrived +1，意味着当前阶段多了一个必须到达的人。如果你的意图是"下一阶段再加入"，必须等当前阶段结束再 register，Phaser 没有提供"注册到下一阶段"的 API。

4. terminated 之后不抛异常，静默返回负数。arrive 返回负、awaitAdvance 立刻返回、register 返回负数且不生效。代码看起来正常跑完，实际什么都没同步。长期运行的服务里，`onAdvance` 的终止条件写错（比如 `phase >= N` 里 N 被算成 0）会让整个同步机制静默失效，这是最难排查的一类 bug。

5. phase 会回绕。phase 存在 31 位里，到 `Integer.MAX_VALUE` 之后加 1 变负数，源码用 `& 0x7fffffff` 拉回 0。所以 `onAdvance` 里比较 phase 别用 `==`，用 `>=`，或者干脆依赖 terminated 位。

**记住这一句**：Phaser 把"等几个人"和"走到第几步"合并成一个可变的 long，于是同步器本身也成了可以被同步的对象。
