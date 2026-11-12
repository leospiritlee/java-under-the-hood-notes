# 第1章 内存模型：并发的一切从这里开始

**一句话**：JMM 用主内存/工作内存抽象掉硬件差异，只对同步过的读写承诺结果。

**为什么需要它**：

先看一段谁都会写的代码。

```java
static boolean stop = false;

// 线程 A
new Thread(() -> {
    while (!stop) {
        // 干活
    }
}).start();

// 线程 B，一秒钟后
stop = true;
```

在单核机器、解释执行模式下，这段代码能正常退出。换到一台多核服务器，让 C2 把循环编译到极致，A 线程可能永远不退出。更诡异的是，你在循环体里加一行 `System.out.println`，它又好了——因为 `println` 内部有 `synchronized`，顺带让 `stop` 重新从主内存读了一次。

"加一行日志就好了"这类现象说明的不是 JVM 有 bug，而是这段代码从一开始就没有确定的语义，之前只是碰巧跑对了。并发程序的可预测性不是自然属性，是被规范授予的。

这一章要解决的不是"怎么开线程"，而是一个判定问题：给定一段多线程代码，你能不能事先判断它有没有唯一的执行结果；`volatile`、`synchronized`、`final` 这三个关键字各自买到了什么、明确没买到什么。判定清楚了，后面所有并发工具——`AQS`、`ConcurrentHashMap`、`ThreadPoolExecutor`——的源码才读得动，因为它们每一个的注释里都在引用同一套规则。

**核心结构**：

| 概念 | 作用 |
|---|---|
| 主内存（main memory） | 存放所有实例字段、静态字段、数组元素；线程之间唯一的交换场所 |
| 工作内存（working memory） | 每个线程私有的抽象，寄存器、L1/L2、store buffer、编译器临时值的统称，不是一块真实内存 |
| 重排序（reordering） | 编译器、CPU、内存系统三层机制，共同破坏"代码顺序 = 执行顺序"这个假设 |
| 数据依赖 | 单线程内重排序的边界：有依赖不能换，无依赖可以随便换 |
| 内存屏障（memory barrier） | 禁止屏障两侧特定类型的重排，是 JMM 语义落到指令上的手段 |
| volatile | 写具 release 语义、读具 acquire 语义；不提供互斥，不提供复合操作原子性 |
| synchronized | monitorenter 等价 acquire、monitorexit 等价 release；额外提供互斥 |
| final | 构造方法退出时 freeze，禁止 final 字段的写被重排到引用发布之后 |

**关键代码**：

四个变量，两个线程，各两行，能跑出一个"不可能"的结果。

```java
// x = y = a = b = 0，两个线程同时起跑
// Thread 1                Thread 2
a = 1;                     b = 1;
x = b;                     y = a;
// 直觉：x 和 y 至少有一个是 1
// 实测：跑几万次能抓到 (x, y) = (0, 0)
```

要得到 `x == 0 && y == 0`，必须让两个线程各自的第二行都跑到第一行前面。三条独立路径都能做到，下面拆解里展开。

第二个例子是这三条路径的实战后果——双重检查锁（Double-Checked Locking，DCL）。

```java
public class Singleton {
    private static Singleton instance;   // 少一个 volatile，整个类就是错的
    private int v;

    private Singleton() { this.v = 3; }

    public static Singleton getInstance() {
        if (instance == null) {                    // 第一次检查，无锁
            synchronized (Singleton.class) {
                if (instance == null) {            // 第二次检查
                    instance = new Singleton();    // 这一行在字节码里是三步
                }
            }
        }
        return instance;
    }
}
```

`instance = new Singleton()` 展开后：

```text
new           #2   // 1. 分配内存，所有字段先填 0/null
dup
invokespecial #3   // 2. 执行 <init>，写入 v = 3
putstatic     #4   // 3. 把引用写进 instance
```

第 2 步和第 3 步之间没有数据依赖。JIT 完全可以把 `putstatic` 提到 `invokespecial` 之前——于是别的线程在第 1 步检查时看到 `instance != null`，拿到一个 `v == 0` 的半成品。

**拆解**：

### 一、重排序有三个来源，只有一个和 CPU 乱序有关

回到那个四行程序。要出现 `(0, 0)`，需要 `x = b` 在 `b = 1` 之前读到值，同时 `y = a` 在 `a = 1` 之前读到值。三条路径：

**编译器重排**。`a = 1` 和 `x = b` 之间没有数据依赖，也没有同步约束，交换它们不改变单线程的可观察结果。需要澄清一点：`javac` 几乎不做这种重排，它基本是逐条翻译 AST。真正干这事的是 JIT 的 C2，在 SSA 形式的中间表示上做指令调度，把访存指令往前提以掩盖延迟。

**CPU 乱序执行**。即使字节码原封不动，处理器也可以重排。x86 是 TSO（Total Store Order）模型，只允许一种重排：store 之后的 load 可以被提前。而这里的 `a = 1` 是 store、`x = b` 是 load，正好踩在 TSO 唯一的那个口子上。ARM 更宽松，四种组合里三种都能换。

**内存系统"看起来"乱序**。这一条最容易被忽略，因为它不是重排。指令确实按顺序发射了，`a = 1` 也确实执行了，但它落在 core0 的 store buffer 里，还没合并进缓存行；core1 读 `a` 时读的是自己缓存里的旧副本，拿到 0。顺序没变，可见性没了。

JLS 第 17.4 节把这三者统称为 compiler——原文写的是 "we refer to all of these as compiler"，还特意说明不翻译成"编译器"。这个用词是刻意的：后面写规则时，规范不需要区分到底是谁干的，只关心"结果是否合法"。

这里有一条红线：**单线程语义不可违反**。任何重排都不能改变单线程内可观察的结果。所以重排永远只在"跨线程观察"时才暴露——这也是为什么单线程调试永远复现不出来。

JIT 判定"能不能换"的依据是数据依赖和别名分析。`a = 1; x = b;` 里，前者写的是变量 `a`，后者读的是变量 `b`，两者不构成依赖，交换合法；如果写成 `a = 1; x = a;`，就构成写后读依赖（RAW），不能换。问题在于别名分析只能做到"保守地证明不别名"，证明不了的时候就不优化。而跨线程的共享变量恰恰是最难证明的那一类——两个线程持有同一个对象的引用，编译器在编译其中一个方法时根本看不到另一个线程在干什么。于是它只能按"我看不到就当我独占"的假设来优化，这个假设在单线程里永远成立，在多线程里永远可能不成立。

### 二、可见性不是多核的锅，是私有缓存加写缓冲的锅

一个常见的错误归因：多核导致可见性问题。不对。如果所有核心共享同一份缓存，就不存在可见性问题。问题的根源是每个核心有自己的一级缓存、二级缓存，以及更隐蔽的 store buffer。

MESI 协议保证了缓存行级别的一致性：core0 写了某行，core1 对应的行会被标记为 Invalid。听起来已经够了，但 store buffer 打破了它——写操作先进入本核的 store buffer，CPU 不等它落到缓存就继续往下执行；此时别的核看到的还是旧值。失效消息（invalidation）也有传递延迟，读操作可能命中一条"已经该失效但还没处理完"的行。

JMM 不去描述 MESI、store buffer、invalidation queue，它换了一套完全不同的词：所有共享变量放在**主内存**，每个线程有自己的**工作内存**，线程读写共享变量必须先经过工作内存。

工作内存不是真实内存。它是寄存器、L1/L2、store buffer、编译器优化出来的临时变量的统称。规范选用这个抽象，是因为它必须在所有平台上都成立——包括只有一个核、压根没有缓存的嵌入式 JVM。抽象越粗，覆盖面越广。

这个抽象的收益是：把"硬件细节"换成了"读写规则"。你不需要知道 x86 的 TSO 和 ARM 的弱内存模型差在哪，只需要知道"工作内存里的副本什么时候失效"。

代价是：抽象是单向的。规范只保证它白纸黑字写下来的东西，没写的一律不保证。这也解释了一个常见困惑——为什么 `volatile` 的语义在 JLS 里读起来那么绕，因为它描述的不是"做了什么"，而是"允许你观察到什么"。

那为什么不干脆采用连续一致性（sequential consistency）？那样所有线程看到同一个全局顺序，每个操作原子且立即可见，问题一次性解决。答案是成本：连续一致性直接禁止了编译器和 CPU 的绝大部分优化。一旦 `p.x = 3` 执行，后续任何位置对该字段的读都必须立刻看到 3，JIT 不能再把值缓存在寄存器里，CPU 不能再用 store buffer，连循环里的不变量提升都做不了。这不是"慢一点"，是把现代处理器的性能模型整个废掉。JMM 的选择是：默认放开，只在同步点收口。这就是为什么并发正确性必须由程序员用同步手段主动"购买"，而不是默认赠送。

### 三、synchronized 和 volatile 是同一套语义的两个档位

先把两个关键字的对应关系摆出来：

- 进入 `synchronized`（monitorenter）≈ volatile 读
- 退出 `synchronized`（monitorexit）≈ volatile 写

具体行为：线程拿到监视器锁进入临界区时，它工作内存中该锁保护的共享变量副本失效，后续读必须回主内存取；退出时，把进入之前和临界区内对共享变量的所有写刷回主内存。

注意这里的不对称：**进入时不刷，退出时才刷**。所以"`synchronized` 保证可见性"的准确表述是"解锁 happens-before 后续对同一监视器的加锁"，是"退出 → 进入"这一对配对，不是"进入 → 退出"。如果你在临界区里等一个别的线程写的值，等不到，因为进入时并不会把别人的写拉过来——它只是让你的副本失效，你重新读的时候读到的可能是主内存里尚未更新的旧值。

两者的唯一区别是互斥。`synchronized` 有，`volatile` 没有。所以 `volatile` 便宜：它不涉及操作系统互斥量，不需要在用户态和内核态之间切换，x86 上编译成一条带 lock 前缀的指令（形如 `lock addl $0x0,(%rsp)`，用一个空操作触发缓存行锁定和 store buffer 刷出），ARM 上是一条 `dmb ish`。

第二件事是禁止重排序，而 `volatile` 的禁止范围比大多数人以为的宽得多。它不只是"两个 volatile 操作之间不能换"，而是"volatile 操作和它前后的普通读写也不能随便换"。落地成四条屏障：

```text
volatile 写之前  → StoreStore：前面的普通写必须先落地
volatile 写之后  → StoreLoad ：这个写不能被后面的读越过
volatile 读之后  → LoadLoad + LoadStore：后面的读写不能被提前
volatile 读之前  → 无需屏障（acquire 只约束后面）
```

这四条屏障直接推出一个常被当作"结论"来背的推论：线程 A 写一个 volatile 变量，线程 B 随后读它，那么 A 在这个写之前的所有写，对 B 都可见。推导过程是：A 的普通写 →(程序顺序)→ A 的 volatile 写 →(synchronizes-with)→ B 的 volatile 读 →(程序顺序)→ B 的普通读。四次传递，链条闭合。它不是一条独立规则，是屏障语义加传递性的算术结果。

这里必须把原子性单独拎出来说，因为它是和可见性正交的第二个轴，混淆这两者是并发 bug 的主要来源。JLS 17.7 规定：对 non-volatile 的 `long` 和 `double` 的写，允许拆成两个相邻的 32 位写。一个线程写高 32 位、另一个线程写低 32 位，第三个线程就可能读到一个从未存在过的组合值——这就是字分裂（word tearing）的近亲。`volatile long` 和 `volatile double` 被强制要求原子，因为 volatile 语义本身就蕴含"这一次写要么全发生要么全不发生"。引用类型的读写在任何位宽上都是原子的，32 位机器上也是如此，这一点常被误以为需要 64 位才成立。

规范对此的措辞是"鼓励"（encourage）JVM 实现避免拆分 64 位写，不是"要求"。实际上 64 位 HotSpot 上 `long` 写本来就是原子的，但这是实现选择，不是语言承诺。把它当承诺写进代码，换一个 JVM 或换一个平台就可能出问题。原子性解决"读到的值是不是完整"，可见性解决"读到的值是不是新的"，两件事没有任何蕴含关系。

### 四、DCL 坏在构造函数不是原子的

回到字节码那三步。第 2 步 `invokespecial` 执行构造方法，第 3 步 `putstatic` 发布引用。两者之间没有数据依赖，所以第 3 步可以被提到第 2 步之前；即使不被提，第 2 步里的 `v = 3` 也可能还留在 store buffer 里没刷出去。

于是线程 B 在第 1 步检查时读到 `instance != null`，跳过了锁，直接返回一个 `v == 0` 的对象。这就是"部分构造"（partially constructed object）。

加 `volatile` 为什么能修好：`putstatic` 变成 volatile 写，它的 release 语义要求之前的所有写——包括 `<init>` 里的 `v = 3`——必须先完成。构造没结束，引用就发不出去。

有人试过绕开 `volatile`，用 `synchronized` 的可见性来凑：

```java
public static Singleton getInstance() {
    if (instance == null) {
        Singleton temp;
        synchronized (Singleton.class) {
            temp = instance;
            if (temp == null) {
                synchronized (Singleton.class) {   // 内嵌一层
                    temp = new Singleton();
                }
                instance = temp;                   // 在锁外
            }
        }
    }
    return instance;
}
```

思路是：内层锁退出时，`temp` 一定已经完整构造。这个前提没错。错在下一步——规范只保证"解锁之前的代码在解锁之前执行完"，没有任何规则禁止"解锁之后的代码被提前到解锁之前执行"。`instance = temp` 在锁外，完全可以被提到内层锁里面去，重排问题原封不动地复现。

最后一个例外：如果 `Singleton` 的所有字段都是 `final`，不加 `volatile` 也是安全的。原因是 final 字段在构造方法退出时会执行 freeze 操作，freeze 之前的写不能被重排到引用发布之后。但前提很硬——`this` 不能在构造期间逸出：注册监听器、塞进静态集合、传给别的线程，任何一个都会让 freeze 的保证失效。

规范给过一个对照实验，把这件事说得很干净：

```java
class FinalFieldExample {
    final int x;
    int y;
    static FinalFieldExample f;

    FinalFieldExample() { x = 3; y = 4; }

    static void writer() { f = new FinalFieldExample(); }

    static void reader() {
        if (f != null) {
            int i = f.x;   // 必然是 3
            int j = f.y;   // 可能是 0
        }
    }
}
```

同一个构造方法里写的两个字段，一个能保证、一个不能，差别只在 `final`。因为 `x = 3` 发生在 freeze 之前，而 `f` 的赋值（在 `writer` 里，构造完成之后）发生在 freeze 之后，freeze 把它们钉死在两侧。`y = 4` 没有 freeze 保护，它可以被延迟到引用发布之后才落地，读者看到 0。

顺带说一句 `final` 的实际用途比 DCL 大得多：`String` 的 `value`、`offset`、`count` 全是 final，`String` 的不可变性正是靠这个在数据竞争环境下也成立——即使恶意代码在无同步的情况下把 `String` 引用传来传去，`substring` 出来的对象也不会出现 `offset` 一半新一半旧的撕裂状态。这是 Java 安全模型的一块基石，不是语法糖。

**容易误解的地方**：

1. 很多人以为 `volatile` 是"开关缓存、强制每次读写主内存"，其实是 acquire/release 语义加内存屏障；实现上靠一条 lock 前缀指令或 `mfence`，而不是真的每次都走到 DRAM。
2. 很多人以为 `synchronized` 进入时会把之前的写刷进主内存，其实只有退出时才刷；进入时做的是让本地副本失效，方向相反。
3. 很多人以为 `volatile` 可以替代 `synchronized`，其实它没有互斥性，`i++` 这种读-改-写照样丢更新，因为三条字节码指令之间可以被插入。
4. 很多人以为 64 位 JVM 上 `long` 写天然原子就没问题了，其实原子性和可见性是两个正交的问题，原子只保证你不会读到半个值，不保证你读到新值。

**记住这一句**：JMM 从不承诺你的代码按写的顺序执行，它只承诺在有同步关系的地方，你读到的值是对的。
