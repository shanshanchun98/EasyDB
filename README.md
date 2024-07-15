# MYDB

MYDB 是一个 Java 实现的简单的数据库，部分原理参照自 MySQL、PostgreSQL 和 SQLite。实现了以下功能：

- 数据的可靠性和数据恢复
- 两段锁协议（2PL）实现可串行化调度
- MVCC
- 两种事务隔离级别（读提交和可重复读）
- 死锁处理
- 简单的表和字段管理
- 简陋的 SQL 解析（因为懒得写词法分析和自动机，就弄得比较简陋）
- 基于 socket 的 server 和 client

## 运行方式

首先执行以下命令以 /tmp/mydb 作为路径创建数据库：

```shell
mvn exec:java -Dexec.mainClass="top.guoziyang.mydb.backend.Launcher" -Dexec.args="-create /tmp/mydb"
```

随后通过以下命令以默认参数启动数据库服务：

```shell
mvn exec:java -Dexec.mainClass="top.guoziyang.mydb.backend.Launcher" -Dexec.args="-open /tmp/mydb"
```

这时数据库服务就已经启动在本机的 9999 端口。重新启动一个终端，执行以下命令启动客户端连接数据库：

```shell
mvn exec:java -Dexec.mainClass="top.guoziyang.mydb.client.Launcher"
```

会启动一个交互式命令行，就可以在这里输入类 SQL 语法，回车会发送语句到服务，并输出执行的结果。

一个执行示例：

![](https://s3.bmp.ovh/imgs/2021/11/2749906870276904.png)  


## 原理梳理
### 整体流程
![模块架构图](https://ziyang.moe/wp-content/uploads/2022/04/006VKfGmly8gwtst971hvj30bx0feq34.jpg)  
**各模块功能及依赖总结**  
通过几个例子, 来看一下各个模块之间是怎么相互工作的.  
总结来自NYADB作者，针对声哥的源码进行简单修改。https://qw4990.gitbooks.io/nyadb/content/chapter7.html

先总结一下各个模块提供的操作  
– DM: insert(x), update(x), read(x)  
DM提供了针对数据项(data item)的基本插入, 更新, 读取操作, 且这些操作是原子性的. DM会直接对数据库文件进行读写.  
– TM: begin, commit(T), abort(T), isActive(T),isCommitted(T),isAborted(T)  
TM提供了针对事务的开始, 提交, 回滚操作, 同时提供了对数据项状态的查询操作，XID为事务id.  
– VM: insert(X), update(X), read(X), delete(X)  
VM提供了针对记录(entry)的增删查改操作, VM在内部为每条记录维护多个版本, 并根据不同的事务, 返回不同的版本. VM对这些实现, 是建立在DM和TM的各个操作上的，还有一个事务可见性类Visibility。  
– TBM: execute(statement)  
TBM就是非常高层的模块了, 他能直接执行用户输入的语句(statement), 然后进行执行. TBM对语句的执行是建立在VM和IM提供的各个操作上的.  
– IM: value search(key), insert(key, value)    
IM提供了对索引的基本操作.  

read语句的流程  
假设现在要执行read * from student where id = 2012141461290, 并且在id上已经建有索引. 执行过程如下:    
1、TBM接受语句, 并进行解析.  
2、TBM调用IM的search方法, 查找对应记录所在的地址.  
3、TBM调用VM的read方法, 并将地址作为参数, 从VM中尝试读取记录内容.  
4、VM通过DM的read操作, 读取该条记录的最新版本.  
5、VM检测该版本是否对该事务可见, 其中需要Visibility.isVisible()方法.  
6、如果可见, 则返回该版本的数据.  
7、如果不可见, 则读取上一个版本, 并重复5, 6, 7.  
8、TBM取得记录的二进制内容后, 对其进行解析, 还原出记录内容.  
9、TBM将记录的内容返回给客户端.

insert语句的流程  
假设现在要执行insert into student values “zhangyuanjia” 2012141461290这条语句. 执行过程如下:  
1、TBM接受语句, 并进行解析. 
2、TBM将values的值, 二进制化.  
3、TBM利用VM的insert操作, 将二进制化后的数据, 插入到数据库.  
4、VM为该条数据建立版本控制, 并利用DM的insert操作, 将数据插入到数据库.  
5、DM将数据插入 到数据库, 并返回其被存储的地址.  
6、VM将得到的地址, 作为该条记录的handler, 返回给TBM.  
7、TBM计算该条语 句的key, 并将handler作为data, 并调用IM的insert, 建立索引.  
8、IM利用DM提供的read和insert等操作, 将key和data存入 索引中.  
9、TBM返回客户端插入成功的信息.  

### DM模块梳理
先谈每个子类的功能：

1、AbstractCache：引用计数法的缓存框架，留了两个从数据源获取数据和释放缓存的抽象方法给具体实现类去实现。  
2、PageImpl：数据页的数据结构，包含页号、是否脏数据页、数据内容、所在的PageCache缓存。  
3、PageOne：校验页面，用于启动DM的时候进行文件校验。  
4、PageX：每个数据页的管理器。initRaw()新建一个数据页并设置FSO值，FSO后面存的其实就是一个个DataItem数据包  
5、PageCacheImpl：数据页的缓存具体实现类，除了重写获取 和释放两个方法外，还完成了所有数据页的统一管理：  
1）获取数据库中的数据页总数；getPageNumber()  
2）新建一个数据页并写入数据库文件；newPage(byte[] initData)  
3）从缓存中获取指定的数据页；getPage(int pgno)  
4）删除指定位置后面的数据页；truncateByBgno(int maxPgno)  
6、PageIndex：方便DataItem的快速定位插入，其实现原理可以理解为HashMap那种数组+链表结构（实际实现是 List+ArrayList），先是一个大小为41的数组 存的是区间号（区间号从1>开始），然后每个区间号数组后面跟一个数组存满足空闲大小的所有数据页信息（PageInfo）。  
7、Recover：日志恢复策略，主要维护两个日志：updateLog和insertLog，重做所有已完成事务 redo，撤销所有未完成事务undo  
8、DataManager：统揽全局的类，主要方法也就是读写和修改，全部通过DataItem进行。  

再梳理流程：  

首先从DataManager进去创建DM（打开DM就不谈了，只是多了个检验PageOne 和更新PageIndex），需要执行的操作是：  
1）新建PageCache，DM里面有 页面缓存 和 DataItem缓存 两个实现；
DataItem缓存也是在PageCache中获取的，DataItem缓存不存在的时候就去PageCache缓存获取，PageCache缓存没有才去数据库文件中获取；  
2）新建日志，  
3）构建DM管理器；  
4）初始化校验页面1： dm.initPageOne()nnnDataManager的所有功能（主要功能就是CRUD，进行数据的读写修改都是靠DataItem进行操作的 ，所以PageX管理页面的时候FSO后面的DATA其实就是一个个的DataItem包）：  
1、初始化校验页面1：  
initPageOne() 和 启动时候进行校验：loadCheckPageOne()  
2、读取数据 read(long uid)：  
从DataItem缓存中读取一个DataItem数据包并进行校验，如果DataItem缓存中没有就会调用 DataManager下的getForCache(long uid)从PageCache缓存中读取DataItem数据包并加入DataItem缓存（其实PageCache缓存和DataItem缓存都是共用的一个cache Map存的，只是key不一样，page的key是页号，DataItem的key是uid，页号+偏移量），如果PgeCache也没有就去数据库文件读取。  
3、插入数据 insert(long xid, byte[] data)：
先把数据打包成DataItem格式，然后在 pageIndex 中获取一个足以存储插入内容的页面的页号； 获取页面后，需要先写入插入日志Recover.insertLog(xid, pg, raw)，接着才可以通过 pageX 在目标数据页插入数据PageX.insert(pg, raw)，并返回插入位置的偏移。如果在pageIndex中没有空闲空间足够插入数据了，就需要新建一个数据页pc.newPage(PageX.initRaw())。最后需要将页面信息重新插入 pageIndex。  
4、修改数据就是先读取数据，然后修改DataItem内容，再插入DataItem数据。但是在修改数据操作的前后需要调用DataItemImp.after()进行解写锁并记录更新日志，这里需要依赖DataManager里面的logDataItem(long xid, DataItem di)方法；  
5、释放缓存：  
释放DataItem的缓存，实质上就是释放DataItem所在页的PageCache缓存  
