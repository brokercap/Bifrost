## Bifrost ---- 面向生产环境的 MySQL,MariaDB 同步到Redis,ClickHouse,Elasticsearch等服务的异构中间件

[![Gitter](https://badges.gitter.im/brokercap-Bifrost/Bifrost.svg)](https://gitter.im/brokercap-Bifrost/Bifrost?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge)
[![Build Status](https://app.travis-ci.com/brokercap/Bifrost.svg?branch=v1.8.x)](https://app.travis-ci.com/github/brokercap/Bifrost)
[![License](https://img.shields.io/github/license/jc3wish/Bifrost.svg)](https://opensource.org/licenses/apache2.0)

[English](https://github.com/brokercap/Bifrost/blob/v1.8.x/README.EN.MD)

##### 漫威里的彩虹桥可以将 雷神 送到 阿斯加德 和 地球


#### 而这个 Bifrost 可以将数据 <font color="#F00">全量</font> , <font color="#F00">实时</font>的同步到 多个目标端


##### 目标端支持

| 目标端        | DDL | 备注                     |
|------------|-----|------------------------|
| Redis      | NO  |                        |
| MongoDB    | NO  |                        |
| MySQL      | YES |                        |
| ClickHouse | YES |                        |
| Memcache   | NO  |                        |
| RabbitMQ   | YES |                        |
| Kafka      | YES | 支持canal,debezium格式，以及kafka 消息字符串 |
| ActiveMQ   | YES |                        |
| [Http 自定义服务](https://github.com/brokercap/Bifrost/blob/v1.8.x/plugin/http/example/http_server/http_server.go)| YES  |                        |
| [Hprose RPC 自定义服务](https://github.com/brokercap/Bifrost/blob/v1.8.x/hprose_server/tcp_server.go)| YES  |                        |
| StarRocks        | YES | use mysql protocol     |
| Doris            | YES | use mysql protocol     |



##### 源端支持

| 源端      | 全量  | 增量  | 格式               | 备注                                         |
|---------|-----|-----|------------------|--------------------------------------------|
| MySQL   | YES | YES | Binglog & Select | 模式MySQL Slave&全版本                          |
| MariaDB | YES | YES | Binglog & Select | 模式MySQL Slave&全版本                          |
| Percona | YES | YES | Binglog & Select | 模式MySQL Slave&全版本                          |
| Kafka   | NO  | YES | string           | 将kafka数据作为一个queue_data字段进行处理               |
| Kafka   | NO  | YES | canal            | 将canal的格式自动转成biforst格式进行处理                 |
| kafka   | NO  | YES | bifrost          | Bifrost可以作为源写入到kafka,再由Bifrost消费kafka的数据   |
| kafka   | NO  | YES | debezium         | debezium将数据写入kafka，Bifrost消费kafka的数据自动转换处理 |
| Mongo   | NO  | YES | oplog            |                                            |
| kafka   | NO  | YES | customer json    | 支持自定义JSON格式数据转换Bifrost格式的功能                |
| Mock    | NO  | YES | MySQL Binlog     | 对于OutPut插件开发的时候 ,不再需要部署MySQL等源端进行开发阶段的测试了  |

---

#### **WIKI** : [https://wiki.xbifrost.com](https://wiki.xbifrost.com)

----

#### Bifrost 特点

* 支持 MySQL,MariaDB 所有存储类型

* **界面化**动态灵活配置不同的数据表及目标库

* 多种数据源,**多种目标库**支持

* 同时支持 **增量** 及 **全量** 数据同步

* 一个Binlog解析线程,多个目标库**并行同步**

* 宕机后重启自动恢复配置,位点精确控制

* 支持 Email 及 微信企业号等**监控报警**功能

* 提供第三方监控平台访问接口(Http Basic验证)[HTTP API](https://wiki.xbifrost.com/api/http_api/)

* MySQL,ClickHouse 插件支持  DDL 同步

* 简单即可实现[插件开发](https://wiki.xbifrost.com/plugin/plugin_dev/)

#### 源码下载

[Github](https://github.com/brokercap/Bifrost/)  , [Gitee](https://gitee.com/jc3wish/Bifrost)

你的点击 star 就是对 Bifrost 最大的支持!!!


#### Bifrost在线界面体验

https://www.xbifrost.com/demo/goto.html

用户名： Bifrost

密码  :  Bifrost123

![image](https://github.com/brokercap/Bifrost/blob/v1.8.x/bifrost.png)


#### *Bifrsot* Diagram

![image](https://github.com/brokercap/Bifrost/blob/v1.8.x/diagram.png)


#### MySQL 版本（经过测试）

* 5.1.73

* 5.5.42

* 5.6.36

* 5.7.18

* 8.0.21


#### 安装

##### 编译

```sh

git clone https://github.com/brokercap/Bifrost.git

make install prefix=./target
```

##### 二进制文件安装
`````sh

wget https://github.com/brokercap/Bifrost/releases/download/v2.3.6-beta/bifrost_v2.3.6-beta_Linux-amd64-bin.tar.gz

tar -zxvf bifrost_v2.3.6-beta_Linux-amd64-bin.tar.gz

cd bifrost_v2.3.6-beta_Linux-amd64-bin/bin && chmod a+x ./Bifrost*

`````

##### 启动

`````shell
./Bifrost-server start

`````

##### 停止

`````shell
./Bifrost-server stop

`````


##### 界面管理

https://127.0.0.1:21036

用户名和密码是在 etc/Bifrost.ini 配置文件中配置的

默认用户名和密码

用户名：Bifrost

密码：Bifrost123


##### Docker启动

`````shell
docker pull jc3wish/bifrost

mkdir -p /data/BifrostData

docker run -d -p21036:21036 -v /etc/localtime:/etc/localtime -v /data/BifrostData:/linux/data jc3wish/bifrost

`````


##### 配置
`````sh

vim Bifrost.ini

[user]
#登入用户名和密码
#用户名在前，= 号后面是密码
Bifrost=Bifrost123
BifrostMonitor=Bifrost123

[groups]
#administrator 管理人员可以添加删除等操作
#monitor 只能查看数据
Bifrost=administrator
BifrostMonitor=monitor

[Bifrostd]
#log写到入的文件夹，默认为启动文件所在文件夹下的logs目录
#log_dir=/data/log/

#监听端口,默认 0.0.0.0:21036
listen=0.0.0.0:21036

#通过界面配置的信息保存文件夹,默认为启动文件所在文件夹下的data目录
data_dir=./

#设置同步给第三方服务的队列大小，默认5000
toserver_queue_size=10000

#通道队列的大小，默认1000
channel_queue_size=1000

#是否支持动态加plugin so 插件，默认为false
#这个参数只在 linux 下有效，其他平台全部为false
dynamic_plugin=false

#是否支持https
tls=true

#server.key 及 server.crt 相对于 Bifrost 的路径，也可以填绝对路径
tls_key_file=./etc/server.key
tls_crt_file=./etc/server.crt

#是否开启文件队列功能 true|false
file_queue_usable=false

#统计是否启文件队列的时间，单位毫秒
file_queue_usable_count_time_diff=5000

#file_queue_usable_count_time_diff 时间内内存队列被挤满的次数
file_queue_usable_count=10

#采用什么方式存储,默认文件方式
meta_storage_type=redis

#存储介质的连接地址
meta_storage_path=127.0.0.1:6379

#用于区别实例的名字
cluster_name=bifrostTestClusterName

#是否开启文件队列功能 true|false
file_queue_usable=false

#统计是否启文件队列的时间，单位毫秒
file_queue_usable_count_time_diff=5000

#file_queue_usable_count_time_diff 时间内内存队列被挤满的次数
file_queue_usable_count=10

#在没有数据的情况下,间隔多久提交一次插件,单位 秒
plugin_commit_timeout=5

#在同步出错的情况下,每2次重试之后 间隔多久再重试 ,单位 秒
plugin_sync_retry_time=5

`````

##### 生成https证书

[参考证书生成](https://wiki.xbifrost.com/install/https_certificate/)

### 感谢相关依赖包提供者

- github.com/gmallard/stompngo
- github.com/hprose/hprose-golang
- github.com/Shopify/sarama
- github.com/bradfitz/gomemcache/memcache
- gopkg.in/mgo.v2
- github.com/streadway/amqp
- github.com/go-redis/redis/v8
- github.com/syndtr/goleveldb/leveldb
- github.com/ClickHouse/clickhouse-go
- github.com/olivere/elastic/v7

### QQ群号： 70331601

<img src="https://github.com/brokercap/Bifrost/blob/master/qq.jpg" width="200" alt="qq群"/>

### 捐赠

<table>
    <tr>
        <td><img src="https://wiki.xbifrost.com/images/donate/alipay.jpeg" width="200"></td>
        <td><img src="https://wiki.xbifrost.com/images/donate/weixin.jpeg" width="200"></td>
    </tr>
</table>
