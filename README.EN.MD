
## Bifrost ---- A heterogeneous middleware which can synchronize MySQL/MariaDB binlog data To Redis,MongoDB,ClickHouse,Elasticsearch...

##### *Real time data synchronization, only use mouse .....*

[![Gitter](https://badges.gitter.im/brokercap-Bifrost/Bifrost.svg)](https://gitter.im/brokercap-Bifrost/Bifrost?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge)
[![Build Status](https://app.travis-ci.com/brokercap/Bifrost.svg?branch=v1.8.x)](https://app.travis-ci.com/github/brokercap/Bifrost)
[![License](https://img.shields.io/github/license/jc3wish/Bifrost.svg)](https://opensource.org/licenses/apache2.0)

[中文](https://github.com/brokercap/Bifrost/blob/v1.8.x/README.MD)

Binary [Linux](https://github.com/brokercap/Bifrost/releases) , [mac](https://github.com/brokercap/Bifrost/releases) , [windows](https://github.com/brokercap/Bifrost/releases) , [freebsd](https://github.com/brokercap/Bifrost/releases)

Source: [Github](https://github.com/brokercap/Bifrost/)  , [Gitee](https://gitee.com/jc3wish/Bifrost)

Your star is the biggest support for Bifrost!!!

##### TO

| ToServer                                                                                                | DDL | Notes                                           |
|---------------------------------------------------------------------------------------------------------|-----|-------------------------------------------------|
| Redis                                                                                                   | NO  |                                                 |
| MongoDB                                                                                                 | NO  |                                                 |
| MySQL                                                                                                   | YES |                                                 |
| ClickHouse                                                                                              | YES |                                                 |
| Memcache                                                                                                | NO  |                                                 |
| RabbitMQ                                                                                                | YES |                                                 |
| Kafka                                                                                                   | YES | support canal json,debezium json, string data   |
| ActiveMQ                                                                                                | YES |                                                 |
| [Http](https://github.com/brokercap/Bifrost/blob/v1.8.x/plugin/http/example/http_server/http_server.go) | YES  |                                                 |
| [Hprose RPC](https://github.com/brokercap/Bifrost/blob/v1.8.x/hprose_server/tcp_server.go)              | YES  |                                                 |
| [Hprose RPC](https://github.com/brokercap/Bifrost/blob/v1.8.x/hprose_server/tcp_server.go)              | YES  |                                                 |
| StarRocks                                                                                               | YES | use mysql protocol                              |
| Doris                                                                                                   | YES | use mysql protocol                              |


##### FROM

| 源端      | 全量  | 增量  | 格式               | 备注                                |
|---------|-----|-----|------------------|-----------------------------------|
| MySQL   | YES | YES | Binglog & Select | MySQL Slave                       |
| MariaDB | YES | YES | Binglog & Select | MySQL Slave                       |
| Percona | YES | YES | Binglog & Select | MySQL Slave                       |
| Kafka   | NO  | YES | string           | kafka data to field "queue_data"  |
| Kafka   | NO  | YES | canal            | canal json to bifrost data        |
| kafka   | NO  | YES | bifrost          | bifrost to kafka,kafka to bifrost |
| kafka   | NO  | YES | debezium         | debezium for mysql data           |
| Mongo   | NO  | YES | oplog            |                                   |
| kafka   | NO  | YES | customer json    | customer json to bifrost struct   |
| Mock    | NO  | YES | MySQL Binlog     | use for outut plugin develop      |

---

#### **WIKI** : [https://wiki.xbifrost.com/en](https://wiki.xbifrost.com/en)

----

#### Online experience

http://www.xbifrost.com/demo/

![image](https://github.com/brokercap/Bifrost/blob/v1.8.x/bifrost.png)


#### *Bifrsot* Diagram

![image](https://github.com/brokercap/Bifrost/blob/v1.8.x/diagram.png)


### Points


* Support all save types of MySQL/MariaDB

* Interface **dynamics** can dispose different data sheet and target database flexibly

* Supported by various data sources and target databases

* Supported **FULL** AND **Increment**

* Supported Email AND Wechat **warning**

* Supported **Flow** display

* Plugin MySQL and ClickHouse suppoted DDL


### MySQL version(after test)

* 5.1.73

* 5.5.42

* 5.6.36

* 5.7.18

* 8.0.11


### Install

##### Compile

download Bifrost Source

```
git clone git clone https://github.com/brokercap/Bifrost.git

./build.sh linux

./build.sh windows

./build.sh freebsd

./build.sh darwin
```

After compiling, the corresponding platform name folder will be created in the tags directory,

##### Binary

```
wget https://github.com/brokercap/Bifrost/releases/download/v2.3.6-beta/bifrost_v2.3.6-beta_Linux-amd64-bin.tar.gz

tar -zxvf bifrost_v2.3.6-beta_Linux-amd64-bin.tar.gz

cd bifrost_v2.3.6-beta_Linux-amd64-bin/bin && chmod a+x ./Bifrost*

```


##### Start

`````shell
./Bifrost-server start

`````

##### Stop

`````shell
./Bifrost-server stop

`````


##### manager

https://127.0.0.1:21036

Default user name : Bifrost


Default user password : Bifrost123


##### Docker

`````shell
docker pull jc3wish/bifrost

mkdir -p /data/BifrostData

docker run -d -p21036:21036 -v /data/BifrostData:/linux/data jc3wish/bifrost

`````


##### Configuration
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
toserver_queue_size=5000

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

#采用什么方式存储,默认文件方式
meta_storage_type=redis

#存储介质的连接地址
meta_storage_path=127.0.0.1:6379

#用于区别实例的名字
cluster_name=bifrostTestClusterName

`````

##### build https certificate

[example](https://github.com/brokercap/Bifrost/wiki/HTTPS%E8%AF%81%E4%B9%A6)



### Manager doc

[Bifrost](http://www.xbifrost.com/)

### Thank you to the relevant Elaine package provider

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

<img src="/images/qq.jpg" width="250" alt="qq群"/>