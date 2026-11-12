# 第5章 共享模式：CountDownLatch 和 Semaphore 怎么复用同一套队列

**一句话**：AQS 只规定"负数失败、非负成功、成功后向后传播"，state 是计数还是许可，全由子类定义。

**为什么需要它**：假设不用共享模式，硬拿 ReentrantLock 实现"等 10 个任务跑完"：主线程 lock 10 次，每个工作线程 unlock 一次。第一个问题立刻出现——unlock 抛 `IllegalMonitorStateException`，因为 AQS 的 `tryRelease` 校验 owner，不是持有者不许释放。好，自己写个 Sync 绕过校验，第二个问题紧跟着来：主线程第二次 lock 时 state 已经是重入计数，它根本不会阻塞，"等待"变成了"什么都没等"。再狠一点，把重入也拆掉，第三个问题无解——AQS 的独占释放路径在完全释放时只 `unparkSuccessor(head)` 唤醒一个节点，主线程醒来发现自己还差 9 次 release，而 release 已经不会再来了，队列里剩下的等待者永久挂起。

这不是实现粗糙，是语义错配。独占模式的前提是"锁一次只给一个人，释放一次只对一个人有意义"；而"等 N 个事件"要求"一次状态变化对所有等待者同时生效"。硬塞进独占模式，state 语义和唤醒策略两样都得改，改完它就不叫独占模式了。

这一章要解决的是：AQS 用什么样的最小协议，把"一对多的状态变化"接进同一套 CLH 队列；以及 CountDownLatch（倒计数）和 Semaphore（许可池）如何只覆写一个方法就复用这套机制。

**核心结构**：

| 概念 | 作用 |
|---|---|
| `state` | 共享资源的唯一数字。CountDownLatch 里是剩余未完成数，Semaphore 里是剩余许可数。AQS 不解释它的含义 |
| `tryAcquireShared(arg)` | 子类唯一必须实现的方法。返回值 <0 表示失败，>=0 表示成功，且该返回值会被当作"剩余资源量"交给传播逻辑 |
| `tryReleaseShared(arg)` | 返回 true 表示"这次释放可能让等待者满足"，AQS 据此决定要不要调 `doReleaseShared` |
| `Node.SHARED` | 一个哨兵节点，赋给 `node.nextWaiter`，标记该节点是共享节点，`isShared()` 就是拿它做恒等比较 |
| `doAcquireShared` | 共享模式的入队 + 自旋 + park 循环，和独占版只差两处 |
| `setHeadAndPropagate` | 成功后把自己设为 head，并按条件继续唤醒后继 |
| `doReleaseShared` | 释放侧与传播侧共用的唤醒循环，处理 head 的 SIGNAL / 0 / PROPAGATE 三种状态 |

**关键代码**：

```java
// CountDownLatch.Sync：只回答"门开了没有"，完全不碰 state
protected int tryAcquireShared(int acquires) {
    return (getState() == 0) ? 1 : -1;
}

protected boolean tryReleaseShared(int releases) {
    for (;;) {
        int c = getState();
        if (c == 0)
            return false;                 // 已经开门，再 countDown 是空操作
        if (compareAndSetState(c, c - 1))
            return c - 1 == 0;            // 只有减到 0 的那一次才触发唤醒
    }
}
```

```java
// AQS：共享模式的入队循环，以及成功后的传播
private void doAcquireShared(int arg) {
    final Node node = addWaiter(Node.SHARED);   // ① 和独占版唯一的入队差异
    boolean interrupted = false;
    for (;;) {
        final Node p = node.predecessor();
        if (p == head) {
            int r = tryAcquireShared(arg);
            if (r >= 0) {
                setHeadAndPropagate(node, r);   // ② 不是 setHead，是带传播的 setHead
                p.next = null; // help GC
                return;
            }
        }
        if (shouldParkAfterFailedAcquire(p, node) && parkAndCheckInterrupt())
            interrupted = true;
    }
}

private void setHeadAndPropagate(Node node, int propagate) {
    Node h = head;              // 旧 head，后面还要用
    setHead(node);
    // propagate > 0：还有余量，别人也可能成功
    // h.waitStatus < 0：旧 head 是 SIGNAL（确实有人在等）或 PROPAGATE（有人试图唤醒但扑空）
    if (propagate > 0 || h == null || h.waitStatus < 0 ||
        (h = head) == null || h.waitStatus < 0) {   // 新 head 也可能刚被后继改成 SIGNAL
        Node s = node.next;
        if (s == null || s.isShared())              // 后继为空或也是共享节点，才继续
            doReleaseShared();
    }
}
```

**拆解**：

### state 的两种读法：减到 0 与加减自如

CountDownLatch 的 state 从 N 单调减到 0，是倒计数。注意 `tryAcquireShared` 里一个 `setState` 都没有——await 不消耗资源，只是检查门开没开。这个细节决定了它的行为：state 归零之后，任意多个线程、任意多次 await 全部直接返回，因为检查永远通过。共享模式允许"只读不写的 tryAcquireShared"，这一点和独占模式很不一样。

Semaphore 的 state 是剩余许可，acquire 减、release 加，两个方向都动。它的 `tryAcquireShared` 就是一个 CAS 循环：算出 `remaining = available - acquires`，`remaining < 0` 直接返回负数（不做 CAS，因为注定失败），否则 CAS 提交并返回 `remaining`。

`acquire(n)` 一次要 n 个许可。remaining 不够，返回负数，进队列；remaining 够但 CAS 失败，循环重试；CAS 成功，返回剩余量 r，这个 r 会一路传到 `setHeadAndPropagate`。所以"返回值即剩余量"不是随便定的，它就是传播逻辑的输入：r > 0 说明还有余量，后面排队的共享节点可能也满足，值得继续叫。

这里有个边界值得单独看：`new Semaphore(1)` 被一个线程拿走之后，第二个线程 acquire 时 remaining 恰好等于 0。0 不是负数，按 AQS 的约定算成功——它确实拿到了最后一个许可，只是拿完之后池子空了。返回值 0 传到 `setHeadAndPropagate`，`propagate > 0` 不成立，于是不主动往下叫。这是对的：后面的人一定拿不到，叫了也是白叫，他们会在下一次 release 时被唤醒。同一个返回值，既是"成功与否"的开关，又是"要不要继续传播"的信号，AQS 用两个不同的阈值（>=0 和 >0）把它一分为二。看懂这一点，共享模式的返回值约定就彻底通了。

对比一下 CountDownLatch：它的 `tryAcquireShared` 恒返回 1，永远是"资源无限"的姿态，所以每个醒来的线程都会往下传播，一路传到底。而 Semaphore 的传播是"按余量传"，余量耗尽就停。同一个 `setHeadAndPropagate`，两种子类给出两种传播行为，这就是模板方法的价值。

由此还引出一个必须记住的点：被唤醒不等于拿到资源。Semaphore 里线程被 unpark 后重新执行 `tryAcquireShared`，如果许可在这期间被新来的线程抢走一部分，remaining 又变负，它得继续 park。共享模式的循环天然允许反复失败，这也是它比独占模式更需要传播机制的原因。

再看释放侧。Semaphore 的 `tryReleaseShared` 无条件返回 true（只做溢出检查），意味着每次 release 都触发一次 `doReleaseShared`；CountDownLatch 只有减到 0 那一次返回 true。这个差异直接决定了唤醒频率：Semaphore 是"每次归还都敲一下门"，CountDownLatch 是"最后一票才开门"。

### 入队之后：共享节点和独占节点只差一个标记

`addWaiter(Node.SHARED)` 把 `node.nextWaiter` 指向那个哨兵节点，队列结构本身完全一样：head 是哑节点，每个节点的 waitStatus 由后继在 park 前改成 SIGNAL(-1)。

推演 `new CountDownLatch(2)`，t3、t4 先后 await，t1、t2 各 countDown 一次：

1. t3 入队：`head(ws=0) → t3(SHARED, ws=0)`。
2. t3 自旋，`p == head` 成立但 `tryAcquireShared` 返回 -1；`shouldParkAfterFailedAcquire` 把 head.ws CAS 成 -1，返回 false，不 park；第二次循环进来才真正 park。
3. t4 入队：`head → t3 → t4`，t4 把 t3.ws 改成 -1，然后 park。
4. t1 countDown：state 2→1，`tryReleaseShared` 返回 false，**什么都不发生**。中间状态的 countDown 完全不碰队列，这是最容易被忽略的一点。
5. t2 countDown：state 1→0，返回 true，进 `doReleaseShared`。此时 head.ws == -1，CAS 成 0，`unparkSuccessor(head)` 唤醒 t3。
6. t3 从 `parkAndCheckInterrupt` 返回，重新自旋，`tryAcquireShared` 返回 1，进 `setHeadAndPropagate`。

到第 6 步，如果只做 setHead，t3 走了，t4 还在 park。而 CountDownLatch 的 state 已经是 0，不会再有 release 来唤醒 t4 了。所以必须由 t3 顺手把 t4 叫醒——这就是"向后传播"的全部动机。

顺带说清共享模式对中断的两套处理，这在读源码时经常绕晕。`doAcquireShared`（不可中断版）在 `parkAndCheckInterrupt` 返回 true 时只是把局部变量 `interrupted = true` 记下来，继续自旋，等真正拿到资源后再 `selfInterrupt()` 补一个中断标记——中断被延迟到"成功之后"。`doAcquireSharedInterruptibly` 则是在 park 返回后立刻 `throw new InterruptedException`，走 `finally` 里的 `cancelAcquire` 把节点摘掉。CountDownLatch.await 用后者，因为"门没开"是一个可以安全放弃的等待；Semaphore 两个都提供，`acquire` 可中断、`acquireUninterruptibly` 不可中断，后者在"必须拿到许可才能继续"的场景里避免调用方处理异常。两套代码 90% 相同，差异只在 park 之后那一行——AQS 没有为它做抽象，因为多一层回调的开销比重复十几行更大。

### 传播：为什么醒来的线程有义务叫下一个人

独占模式里"一次释放唤醒一个"是对的：锁只有一个，被唤醒的人拿走，后面的人本来就该等下一次释放。共享模式里"一次状态变化同时满足所有人"才是常态：state 到 0 是对所有 await 的终态，Semaphore 一次 release 出 3 个许可可能满足 3 个等待者。如果只叫一个，而后续没有 release 了，剩下的永久挂起。

`setHeadAndPropagate` 干的事：先把当前节点变成新的哑 head，再判断要不要继续叫 `node.next`。条件里出现了两个 h，含义不同：

- 旧的 h（setHead 之前读的）：它的 waitStatus 是 SIGNAL，说明确实有后继挂在我身上等；是 PROPAGATE(-3)，说明之前有人试图唤醒但没找到人，这个信号还没消费掉。
- 新的 h（`(h = head)` 重新赋值）：setHead 之后，后继可能刚好在自旋里把新 head 的 ws 改成 SIGNAL，说明它已经准备好被 park 了，可以放心叫。

两个条件任意一个成立就调 `doReleaseShared`。而 `doReleaseShared` 本身是个循环，看 head.ws 分支：SIGNAL 就 CAS 成 0 然后 unpark 后继；0 就 CAS 成 PROPAGATE；末尾 `if (h == head) break;`——如果刚才唤醒的线程已经把 head 抢走了（h != head），说明它正在自己往下传播，那这里再跑一轮帮着多推一把，纯粹是吞吐量考虑。CAS 失败也 `continue`，因为很可能就是那个刚被唤醒的线程在同一行代码上抢先了。

于是唤醒变成接力：t2 叫醒 t3，t3 叫醒 t4，t4 发现后面没人了，循环退出。10 个等待者就是 10 次 unpark，不是一次广播。这一点常被误认为"共享模式一次性唤醒全部"，实际不是。

### PROPAGATE：一个专门用来堵丢失唤醒的值

考虑这个时序：

1. 线程 A 执行 release，进 `doReleaseShared`，读到 head.ws == 0——后继刚入队，还没走到 `shouldParkAfterFailedAcquire`，所以还没把 head.ws 改成 -1。
2. A 调 `unparkSuccessor`，但后继此刻还没 park，unpark 打空，这个许可被白白消费掉了。
3. 后继继续执行，把 head.ws 改成 -1，然后 park。
4. 没有下一次 release 了。后继永久挂起。

PROPAGATE 就是补这个洞。A 在 ws == 0 时不 unpark（叫了也没用），而是把 ws CAS 成 -3，留下一个"还有人没被通知"的记号。之后：

- 如果后继在 park 之前又自旋了一次并成功，它走 `setHeadAndPropagate`，条件里 `h.waitStatus < 0` 命中（-3 < 0），继续 `doReleaseShared`，接力传下去。
- 如果后继直接 park 了，`shouldParkAfterFailedAcquire` 会把 -3 改成 -1（规则是 ws <= 0 就 CAS 成 SIGNAL），那么下一次有人 release 时，正常的 SIGNAL 分支会把它叫醒。

所以 PROPAGATE 的价值不在它自己的那个分支，而在于它让 `waitStatus < 0` 这个宽泛判断为真，把"我可能漏了人"这件事编码进 head 的状态里，交给后来者接手。这是整个 AQS 里最像"分布式协议"的一段代码：没有全局锁，靠状态位传递责任。

**容易误解的地方**：

1. "共享模式就是多个线程同时持有锁"。不对。AQS 从不检查谁持有，它只看 state 和 `tryAcquireShared` 的返回值。共享模式的本质是"允许多个线程同时通过同一个检查"，检查内容完全由子类定义。CountDownLatch 的 await 连 state 都不改，谈不上"持有"。

2. "CountDownLatch 可以重置"。不能。state 减到 0 后 `tryReleaseShared` 直接 `return false`，再 countDown 无效；await 永远直接返回。要复用只能 new 一个，或者改用 Phaser。

3. "`doReleaseShared` 一次性唤醒所有等待者"。它只 `unparkSuccessor(head)`，一次一个，靠醒来的线程接力。所以如果某个等待者被中断或抛异常，传播链会断一截，这也是共享模式对中断处理格外小心的原因。

4. "Semaphore 的 release 必须由 acquire 过的线程调用"。没有这个约束。`releaseShared` 不校验 owner，任何线程都能 release，许可数会凭空增长。这是它作为"信号量"而非"锁"的体现，也是误用时最危险的坑——比如在 finally 里无条件 release，异常路径下会把许可池撑爆。

5. 顺带一条：`new Semaphore(n)` 是非公平的。公平模式才多那一行 `hasQueuedPredecessors()`。默认构造下，新来的 acquire 直接 CAS 抢，排队的老线程只能干等。

**记住这一句**：共享模式没有发明新队列，它只给"唤醒"加了一条规矩——谁被满足，谁就有义务把这份满足传给下一个人。
