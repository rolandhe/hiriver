# 什么是hiriver？
hiriver是纯java开发的、高性能的、基于解析mysql row base binlog技术实现的用于监控mysql数据变化并分发这些变化的框架。它提供了一套完整的框架，内置数据监控线程和数据消费线程，对外提供简单的Consumer接口，开发者可以根据自己的业务场景自行实现Consumer接口，而不不必关心线程问题。
## 实现原理
hiriver实现了mysql主从复制协议，把自己伪装成一个mysql的从库，在接收到binlog后按照mysql binlog协议进行解析，由此获取mysql的数据变化。由于基于mysql的主从复制协议，它监控数据变化特别快，理论上与mysql本身的主从同步一样快，甚至更快。同时与在应用层监控数据变化不同，它不需要考虑事务是否成功问题。当然，***限制***是mysql binlog的方式必须是***row***方式。
## 名字的由来
hiriver是hidden river的简称，中文名称"暗渠"，用于隐喻在数据库的后面导流数据，而不必要在应用层做任何控制。

## 支持mysql的版本
hiriver支持mysql 5.6.9+和 mysql5.1+版本。

+ ***强烈推荐*** 使用5.6.9+版本，并使用binlog file name + position的方式处理同步点。
+ 虽然5.6.9+版本提供 ***gtid*** 功能，它是用于表示事务的唯一的id，理论上，基于它可以实现HA功能，当mysql出现故障时可以自动从一台mysql从库切换到另一台，并且不会丢失或者重复数据，***但是*** 在实际的使用过程中gtid依然存在bug，并不稳定，而且存在多个gtid时很难找到mysql认识的初始同步点。
+	mysql5.6.9之前的版本，必须binlog file name和在该文件中的偏移位置作为同步点。
# javadoc
![hiriver组件](https://github.com/rolandhe/doc/blob/master/javadoc/hiriver/hirver/index.html)<br>
![mysql-proto组件](https://github.com/rolandhe/doc/blob/master/javadoc/hiriver/mysql-proto/index.html)
# 使用教程
## quickstart
### 总体说明

1.	hiriver模块组主要由2个组件和一个示例组成：mysql-proto、hiriver和hiriver-sample
2.	mysql-proto实现了mysql的client-server协议，包括Text protocol和主从复制协议。Text protocol是从mysql*正常*读取数据的协议，它是mysql jdbc驱动背后的协议。主从复制协议顾名思义就是实现主从之间复制数据的协议。
3.	hiriver是基于mysql-proto组件封装的监听mysql变化、记录同步点、控制数据消费的上层应用框架。它是hiriver业务流程的实现。它需要与spirng集成使用
4.	hiriver-sample一个使用hirvier的示例

### 准备数据库环境
1. 创建自己的mysql 5.6.28
2. 开启row base和gtid 模式（如果使用gtid作为同步点，必须开启）
	<pre><code>
	log-bin=mysql-bin
	binlog_format=Row
	log-slave-updates=ON
	enforce_gtid_consistency=true
	gtid_mode=ON
	</pre></code>
3. 创建自己的复制账号，创建repl database和一张表，并在表示写入数据

### 快速使用-binlogname + 偏移地址模式
1. 下载代码，找到hiriver-sample模块，它是一个基于spring的web应用,有3 spring xml配置文件，分别是：<pre><code>spring-boot.xml # spring容器描述入口文件</pre></code><pre><code>spring-bin.xml # binlogname + 偏移地址模式</pre></code><pre><code>spring-gtid.xml # gtid模式</pre></code>
2. 修改示例中hiriver-sample.properties的参数,修改数据库相关属性、初始同步点、同步点存储路径和表名过滤黑、白名单配置
3. 初始化同步点使用channel.0000.binlog和channel.0000.binlog.pos属性，可以通过执行 <pre><code>show master status</pre></code>命令获取对应信息![](https://github.com/rolandhe/doc/blob/master/hiriver/hiriver-mysql-bin.png)，修改后如图：![](https://github.com/rolandhe/doc/blob/master/hiriver/hiriver-sample-bin.png)
4. 修改spring-boot.xml中的最后一行为:<pre><code> &lt;import resource="classpath:spring/spring-binlog.xml"/&gt;</pre></code>
3. 使用tomcat/jetty或maven jetty插件运行示例即可
### 快速使用-gtid模式
1. 下载代码，找到hiriver-sample模块，它是一个基于spring的web应用,有3 spring xml配置文件，分别是：<pre><code>spring-boot.xml # spring容器描述入口文件</pre></code><pre><code>spring-bin.xml # binlogname + 偏移地址模式</pre></code><pre><code>spring-gtid.xml # gtid模式</pre></code>
2. 修改示例中hiriver-sample.properties的参数,修改数据库相关属性、初始同步点、同步点存储路径和表名过滤黑、白名单配置，其中channel_0000.gtid参数的配置需要从mysql中查询数获取，执行 <pre><code>show master status</pre></code>命令，得到如下结果：![](https://github.com/rolandhe/doc/blob/master/hiriver/hiriver-mysql-gtid.png),这是一个范围，你只需要使用<pre><code>8c80613e-ac5b-11e5-b170-148044d6636f:1 or 8c80613e-ac5b-11e5-b170-148044d6636f:8</pre></code>即可.修改后如图：![](https://github.com/rolandhe/doc/blob/master/hiriver/hiriver-sample-gtid.png)
3. 修改spring-boot.xml中的最后一行为: <code> &lt;import resource="classpath:spring/spring-gtid.xml"/&gt; </code>
4. 使用tomcat/jetty或maven jetty插件运行示例即可

## 详细参数说明
### 底层socket控制参数（使用TransportConfig类描述）
| 参数名称 | 说明 |
| :------| :------ |
| connectTimeout |  socket连接超时，同Socket.connect(SocketAddress endpoint,int timeout)，单位ms，缺省15000 |
| soTimeout | socket读写超时时间，同Socket.setSoTimeout(int timeout), 单位ms，缺省15000 | 
| receiveBufferSize | socket 接收缓冲区大小，同Socket. setReceiveBufferSize(int size),缺省0，0表示使用系统默认缓冲区大小 | 
| sendBufferSize | socket 接收缓冲区大小，同Socket.setSendBufferSize(int size),缺省0，0表示使用系统默认缓冲区大小 | 
| keepAlive | 是否保持长连接，同Socket.setKeepAlive(boolean on) | 
| initSqlList | 在建立数据库连接后，需要初始化执行sql语句的列表，缺省是仅仅包含"SET AUTOCOMMIT=1" sql语句的列表，该sql在dump mysql binlog时不生效。 TransportConfig 类被dump binlog和执行mysql数据库读取类共用，具体参见 ***重点类说明章节***|

### binlog读取参数（DefaultChannelStream类）
| 参数名称 | 说明 |
| :------| :------ |
| faultTolerantTimeout |  当与mysql失去连接后，线程sleep的时间，超过该时间后再进行重连，单位ms，缺省5000 |
| fetalWaitTimeout | 当读取binlog数据或者解析数据过程中发生未知异常时到下次重试的间隔时间，默认2min，单位ms |
| channelId | dump单个数据库可以理解为是一个数据流，channelId是流的名称，一个hiriver进程中可以支持多个流dump多个数据库，其channelId不能重复，默认是uuid。<br>当一个场景中需要一个进程dump多个数据库时，比如在分库应用中，建议使用channel.0000.id格式命名，其中0000是分库场景中数据库的编号。|
|channelBuffer|用于缓存从数据库dump出数据，DefaultChannelStream 由两个线程组成，一个是provider线程，负责从mysql dump数据，另一个是Consumer线程，负责消费、使用数据，使用channelBuffer 进行数据传递，既可以解耦，又可以提高性能，channelBuffer 不能设置的无限大，需要使用DefaultChannelBuffer.limit属性控制大小|
|channel.buffer.limit| channelBuffer 的大小，对应DefaultChannelBuffer.limit属性，默认5000|
|configBinlogPos|初始同步点，使用BinlogPosition接口描述。支持binlog file name+pos和gtid方式，分别对应于BinlogFileBinlogPosition和GTidBinlogPosition|
|binlogPositionStore|同步点存储，使用BinlogPositionStore接口描述，默认FileBinlogPositionStore实现，可以自由扩展|
|position.store.path|同步点的存储路径，适用与FileBinlogPositionStore 实现，对应FileBinlogPositionStore.filePath属性|
|transactionRecognizer|事务开始、结束识别器，使用TransactionRecognizer描述，针对binlog file name+pos和gtid模式提供BinlogNameAndPosTransactionRecognizer和GTIDTransactionRecognizer实现|
|streamSource|需要dump数据的数据源描述，使用StreamSource接口描述，MysqlStreamSource是针对mysql数据的实现，HAStreamSource是在MysqlStreamSource 之上的封装，它持有多个MysqlStreamSource 对象，当一个发生故障时，它可以自动切换到其他MysqlStreamSource 上，在gtid模式下推荐使用HAStreamSource，这时一般适用于从从库dump数据。|
|slaveHostUrl|从数据库ip:port，对应MysqlStreamSource.hostUrl属性，适用于使用HAStreamSource时|
|table.white|根据表名过滤时的白名单配置，支持正则，参见BlackWhiteNameListTableFilter|
|table.black|根据表名过滤时的黑名单配置，支持正则，参见BlackWhiteNameListTableFilter|

### 数据库配置
| 参数名称 | 说明 |
| :------| :------ |
|user_name|用户名称，对应MysqlStreamSource.userName属性|
|password|密码，对应MysqlStreamSource.password属性|
|hostUrl|数据库ip:port，对应MysqlStreamSource.hostUrl属性|

# 架构设计
## 总体架构
![](https://github.com/rolandhe/doc/blob/master/hiriver/hiriver-arch.png)
## hiriver套件设计
![](https://github.com/rolandhe/doc/blob/master/hiriver/hiriver-frame.png)
## hiriver组件设计
![](https://github.com/rolandhe/doc/blob/master/hiriver/hiriver-compent.png)
### 组件说明
***mysql-proto***组件封装了完整的mysql binlog协议和部分的文本协议（jdbc背后的协议），binlog协议用于dump、解析mysql的binlog日志，文本协议用于执行sql语句、读取表的元数据等。<br>
***hiriver*** 组件基于mysql-proto实现了dump数据、过滤表、解析数据以及消费数据的流程。

# 重点类说明
## 底层通信类
### binlog dump类(BinlogStreamBlockingTransportImpl)
实现mysql binlog dump协议，负责与mysql建立socket连接，完成用户名密码验证后，执行数据dump命令，并持续的读取、解析mysql binlog event数据。
### 数据库数据读取类（TextProtocolBlockingTransportImpl）
mysql文本协议的实现，mysql文本协议即jdbc背后的协议，主要用于执行sql读取数据，也可以执行一些其他的命令，比如读取表定义的元数据等，之所以不使用mysql jdbc是由于两个原因：一是不想引入一个第三方包，降低依赖性；二是mysql的文本协议支持更多指令，比如COM_FIELD_LIST指令方便的获取到表字段是否为空、是否是索引字段等信息，而jdbc是个通用的api，并没有暴露这些指令实现。
## 表名过滤类 （BlackWhiteNameListTableFilter）
支持黑白名单的过滤实现。 
按照表名进行过滤时，表名格式为database.table（可以为正则），以逗号分隔.<br>
当白名单和黑名单同时存在时,只有不在黑名单中同时在白名单中存在的才起作用.<br>
e.g,在properties文件中描述<br>
白名单：filert_white=test.account,test.user_sharding* <br>
白名单：filert_black=test.*bak
## binlog row event数据描述类（BinlogDataSet）
binlog数据是二进制数据，它遵循mysql rowbase binlog协议，在协议内部event作为一个基本单位用于描述数据库的变更，这里的“变更”不仅仅是数据的修改，也可能是事务的开启、结束，表的变更等，在hiriver里我们仅仅关注表数据的变更，BinlogDataSet用于描述一条或多条数据的变化，类似于jdbc的RowSet。BinlogDataSet 包括：<br>
1. channelId <br>
2. sourceHostUrl,该数据来自哪个数据库 <br>
3. gtId， 该数据所在的事务的gtid，在不支持gtid模式下，为null <br>
4. binlogPos, 当前数据所在事务的binlogfile + pos,无论哪种模式，一定补位null <br>
5. isStartTransEvent， 当前是否一个事务的开启 <br>
6. isPositionStoreTrigger,当前是否一个事务的结束，当时true时需要记录同步点。<br>
7. rowDataMap, 行数据,每一行使用BinlogResultRow描述 <br>
8. columnDefMap, 类定义描述<br>

BinlogResultRow内部是有二个列表，一个记录变更之前的数据，另一个记录变更之后的数据。

## 数据消费类 （Consumer）
描述消费BinlogDataSet数据的接口，这个留给业务实现方来实现。

## binlog流(DefaultChannelStream)
mysql binlog dump被抽象成一个流，每一个流仅仅针对一个mysql实例，这个流称之为ChannelStream, ChannelStream负责源源不断的从mysql实例读取数据并过滤、解析和消费。<br>DefaultChannelStream是ChannelStream的缺省实现，在内部它开启了2条线程：provider和consumer线程，provider线程负责从数据库读取数据，识别事务、根据表名过滤、解析成BinlogDataSet并放入ChannelBuffer；consumer线程负责从ChannelBuffer读取数据并调用Consumer进行数据消费。<br> 当provider线程产生数据的速度大于consumer线程消费数据的速度时，数据会被积压在ChannelBuffer中，为了防止内存被打爆，ChannelBuffer需要实现成有界的，当ChannelBuffer达到上限时会阻塞provider线程产生新数据。


## 数据缓存类 （DefaultChannelBuffer）
ChannelStream中provider和consumer线程的数据通信基础，它是ChannelBuffer的缺省实现。谨记，需要配置上限。

## 事务识别类（TransactionRecognizer）
用于识别事务的开启、结束，并且记录当前事务的开始位置。针对gtid和binlog file name + pos两种模式，提供2种实现：GTIDTransactionRecognizer和BinlogNameAndPosTransactionRecognizer。


