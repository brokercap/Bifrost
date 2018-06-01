# Bifrost ---- MySQL到其他服务的高性能异构中间件



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


* 界面化配置不同的数据表

* 一个数据表可以不同的格式同步到多个第三方服务

* 如果数据允许丢失，可以多通道同步

* 单通道同步数据确保安全，以最后一个表同步成功的位点进行保存位点，kill,服务宕机等重启自动恢复配置


### MySQL 版本（经过测试）

* 5.1.73

* 5.5.42

* 5.6.36

* 5.7.18

* 8.0.11



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

wget https://github.com/jc3wish/Bifrost/releases/download/v1.0.2-release/linux.zip

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
 RUN wget https://github.com/jc3wish/Bifrost/releases/download/v1.0.2-release/linux.zip
 RUN unzip linux && cd linux && chmod a+x ./Bifrost-server
 ENTRYPOINT ["linux/Bifrost-server","start"]
 EXPOSE 21036

`````

`````shell
 docker build -t bifrost:1.0.2 .
`````

##### Docker启动
`````shell
docker pull jc3wish/bifrost:1.0.2

docker run -d -p21036:21036 jc3wish/bifrost:1.0.2
`````

##### 管理
http://127.0.0.1:21036


### 界面操作文档
[Bifrost官网](http://www.xbifrost.com/)

### QQ群号： 70331601

