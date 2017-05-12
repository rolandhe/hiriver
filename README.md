# 什么是hiriver？
hiriver是纯java开发的、高性能的、基于解析mysql row base binlog技术实现的用于监控mysql数据变化并分发这些变化的框架。它提供了一套完整的框架，内置数据监控线程和数据消费线程，对外提供简单的Consumer接口，开发者可以根据自己的业务场景自行实现Consumer接口，而不不必关心线程问题。
## 实现原理
hiriver实现了mysql主从复制协议，把自己伪装成一个mysql的从库，在接收到binlog后按照mysql binlog协议进行解析，由此获取mysql的数据变化。由于基于mysql的主从复制协议，它监控数据变化特别快，理论上与mysql本身的主从同步一样快，甚至更快。同时与在应用层监控数据变化不同，它不需要考虑事务是否成功问题。当然，***限制***是mysql binlog的方式必须是***row***方式。
## 名字的由来
hiriver是hidden river的简称，中文名称"暗渠"，用于隐喻在数据库的后面导流数据，而不必要在应用层做任何控制。

## 支持mysql的版本
hiriver支持mysql 5.6.9+和 mysql5.1+版本。

+	***强烈推荐*** 使用5.6.9+版本，5.6.9+版本提供gtid功能，用于表示事务的唯一的id，当在mysql从库上监控变化时，它能够实现HA功能，自动从一台mysql从库切换到另一台，而不会丢失或者重复数据。在该版本下建议使用gtid标示已处理数据的同步点。
+	mysql5.6.9之前的版本，必须binlog file name和在该文件中的偏移位置作为同步点。

# 使用教程
## quickstart
### 总体说明

1.	hiriver模块组主要由2个组件和一个示例组成：mysql-proto、hiriver和hiriver-sample
2.	mysql-proto实现了mysql的client-server协议，包括Text protocol和主从复制协议。Text protocol是从mysql*正常*读取数据的协议，它是mysql jdbc驱动背后的协议。主从复制协议顾名思义就是实现主从之间复制数据的协议。
3.	hiriver是基于mysql-proto组件封装的监听mysql变化、记录同步点、控制数据消费的上层应用框架。它是hiriver业务流程的实现。它需要与spirng集成使用
4.	hiriver-sample一个使用hirvier的示例

### 快速使用-gtid模式
1. 创建自己的mysql 5.6.28
2. 开启row base和gtid 模式
	
	log-bin=mysql-bin
	binlog_format=Row
	log-slave-updates=ON
	enforce_gtid_consistency=true
	gtid_mode=ON
3. 创建自己的复制账号，创建repl database和一张表，并在表示写入数据
4. 修改示例中hiriver-sample.properties的参数,其中channel_0000.gtid参数的配置需要从mysql中查询数，使用show master status命令或者或者show binlog events可以看到gtid，使用show master status时，gtid set类似：8c80613e-ac5b-11e5-b170-148044d6636f:1-13，这是一个范围，你只需要使用8c80613e-ac5b-11e5-b170-148044d6636f:1或者8c80613e-ac5b-11e5-b170-148044d6636f:8即可
5. 运行示例即可

### 快速使用-binlogname + 偏移地址模式
待续 

# 总体架构
![](https://github.com/rolandhe/doc/blob/master/hiriver/hiriver-arch.png)
