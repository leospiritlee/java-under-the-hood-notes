# Java 底层原理精读

> 28 章从线上故障切入 JMM、AQS、容器、GC、NIO 与 Spring

> 原 PDF：[`Java底层原理精读.pdf`](Java底层原理精读.pdf)（仓库根目录，可下载）

## 目录

- [导读（序 / 速查 / 自测）](preface.md)
- [第1章 内存模型：并发的一切从这里开始](ch1.md)
- [第2章 happens-before：JMM 的官方定义](ch2.md)
- [第3章 AQS 骨架：一个 int 和一个双向队列](ch3.md)
- [第4章 公平与非公平：差在那一行判断](ch4.md)
- [第5章 共享模式：CountDownLatch 和 Semaphore 怎么复用同一套队列](ch5.md)
- [第6章 读写锁：读读不互斥是怎么做到的](ch6.md)
- [第7章 Phaser：比 CountDownLatch 更灵活的分阶段同步](ch7.md)
- [第8章 线程池：ctl 这一个 int 装了什么](ch8.md)
- [第9章 线程池：任务从提交到执行的全过程](ch9.md)
- [第10章 BlockingQueue：七种队列差在哪](ch10.md)
- [第11章 RateLimiter：令牌桶的两种实现](ch11.md)
- [第12章 HashMap：从 JDK 7 到 8 的那次改造](ch12.md)
- [第13章 ConcurrentHashMap：从分段锁到 CAS + synchronized](ch13.md)
- [第14章 字符串常量池：intern 到底做了什么](ch14.md)
- [第15章 HotSpot 内存管理：堆、栈、方法区到底怎么分的](ch15.md)
- [第16章 Metaspace 与堆外内存：元数据搬出堆之后](ch16.md)
- [第17章 G1：把堆切成 Region 之后](ch17.md)
- [第18章 ZGC：染色指针与读屏障](ch18.md)
- [第19章 NIO 三件套：Buffer、Channel、Selector](ch19.md)
- [第20章 非阻塞 IO 与异步 IO：差的不只是一个字母](ch20.md)
- [第21章 Tomcat 的 NIO：Acceptor、Poller、Worker](ch21.md)
- [第22章 Netty 启动：Reactor 线程模型是怎么搭起来的](ch22.md)
- [第23章 Netty 内核：Pipeline、ByteBuf 与内存池](ch23.md)
- [第24章 IOC 容器：refresh() 的十二步](ch24.md)
- [第25章 AOP 的演进：从 AspectJ 到动态代理](ch25.md)
- [第26章 Spring AOP 源码：代理是什么时候造出来的](ch26.md)
- [第27章 AspectJ：编译期织入与 @Aspect 的真相](ch27.md)
- [第28章 Sentinel：责任链与滑动窗口](ch28.md)

---

*文字版由 Markdown 源文件按章拆分，内容以原 PDF 为准。*
