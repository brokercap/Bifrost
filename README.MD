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

wget https://github.com/jc3wish/Bifrost/releases/download/v1.0-beta3/Bifrost_v1.0-beta3_bin_linux.zip

unzip Bifrost_v1.0-beta3_bin_linux.zip

cd Bifrost_v1.0-beta3_bin_linux

`````

##### 配置
`````php
[user]
#登入用户名和密码
Bifrost=Bifrost123

[Bifrostd]
#log写到入的文件夹，不配置则默认打印到终端
#log_dir=/data/log/

#监听端口,默认 0.0.0.0:1036
listen=0.0.0.0:9090

#通过界面配置的信息保存文件夹,必须设置
data_dir=/data/bifrost/data

`````

##### 启动
`````shell
./Bifrost -config ./Bifrost.ini

`````
##### 管理
http://127.0.0.1:9090


### 界面操作文档
[Bifrost官网](http://www.xbifrost.com/)

### QQ群号： 70331601

