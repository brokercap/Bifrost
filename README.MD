# Bifrost ---- MySQL到其他服务的高性能异构中间件

[![Build Status](https://travis-ci.org/jc3wish/Bifrost.svg?branch=v1.1.0)](https://travis-ci.org/jc3wish/Bifrost)
[![Join the chat at https://gitter.im/jc3wish-Bifrost/Lobby](https://img.shields.io/badge/GITTER-join%20chat-green.svg)](https://gitter.im/jc3wish-Bifrost/Lobby?utm_source=share-link&utm_medium=link&utm_campaign=share-link)
[![License](https://img.shields.io/github/license/jc3wish/Bifrost.svg)](https://opensource.org/licenses/apache2.0)


*Bifrost* 是一个可以通过界面配置,将 MySQL 数据库中的数据异构的方式同步到Redis,Mongodb,RabbitMQ等甚至自定义RPC服务里的 *简单 高效* 的 异构中间件 .

提供 [Linux](https://github.com/jc3wish/Bifrost/releases) , [mac](https://github.com/jc3wish/Bifrost/releases) , [windows](https://github.com/jc3wish/Bifrost/releases) , [freebsd](https://github.com/jc3wish/Bifrost/releases)  平台二进制可执行文件下载.


*Bifrsot* 支持同步到多种第三方服务, for example:

* Redis
* Memcache
* RabbitMQ
* ActiveMQ
* Kafka
* Mongodb
* Http 自定义服务
* [Hprose RPC 自定义服务](https://github.com/jc3wish/Bifrost/blob/master/hprose_server/tcp_server.go)

![image](https://github.com/jc3wish/Bifrost/blob/master/bifrost1.jpg)


#### *Bifrsot* Diagram

![image](https://github.com/jc3wish/Bifrost/blob/master/diagram.png)


### Bifrost 特点


* 界面化灵活配置不同的数据表

* 一个数据表可以不同的格式同步到多个第三方服务

* kill,服务宕机等重启自动恢复配置

* 每条数据同步完保存位点,数据位点安全,确保不会出现数据重复同步


### MySQL 版本（经过测试）

* 5.1.73

* 5.5.42

* 5.6.36

* 5.7.18

* 8.0.11 (用户密码验证需要采用 mysql_native_password 方式)



### 安装

##### 编译

```sh

go get github.com/jc3wish/Bifrost

# windows
./build.sh windows

# linux
./build.sh linux

#mac
./build.sh darwin

#freebsd
./build.sh freebsd

```

##### 二进制文件下载
`````sh

wget https://github.com/jc3wish/Bifrost/releases/download/v1.0.4-release/linux.zip

unzip linux.zip

cd linux

`````

##### 配置
`````sh

vim Bifrost.ini

[user]
#登入用户名和密码
Bifrost=Bifrost123

[Bifrostd]
#log写到入的文件夹，默认为启动文件所在文件夹下的logs目录
#log_dir=/data/log/

#监听端口,默认 0.0.0.0:21036
listen=0.0.0.0:21036

#通过界面配置的信息保存文件夹,默认为启动文件所在文件夹下的data目录
data_dir=./

`````

##### 启动
`````shell
./Bifrost-server start

`````

##### 停止
`````shell
./Bifrost-server stop

`````

### Docker

##### Dockerfile
`````shell
 FROM centos:centos6.8
 MAINTAINER jc3wish "jc3wish@126.com"
 RUN yum install -y wget unzip
 RUN wget https://github.com/jc3wish/Bifrost/releases/download/v1.0.4-release/linux.zip
 RUN unzip linux && cd linux && chmod a+x ./Bifrost-server
 ENTRYPOINT ["linux/Bifrost-server","start"]
 EXPOSE 21036

`````

`````shell
 docker build -t bifrost:1.0.4 .
`````

##### Docker启动
`````shell
docker pull jc3wish/bifrost:1.0.4

docker run -d -p21036:21036 jc3wish/bifrost:1.0.4
`````

##### 管理
http://127.0.0.1:21036


### 界面操作文档
[Bifrost官网](http://www.xbifrost.com/)

### QQ群号： 70331601
![image](https://github.com/jc3wish/Bifrost/blob/master/qq.jpg)

