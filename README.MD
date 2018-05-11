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

wget https://github.com/jc3wish/Bifrost/releases/download/v1.0-beta3/Bifrost_v1.0-beta4_bin_linux.zip

unzip Bifrost_v1.0-beta4_bin_linux.zip

cd Bifrost_v1.0-beta4_bin_linux

`````

##### 配置
`````sh

vim Bifrost.ini

[user]
#登入用户名和密码
Bifrost=Bifrost123

[Bifrostd]
#log写到入的文件夹，需要获取启的时候指定 -d true 参数才生效，否则默认打印到终端
log_dir=/data/log/

#监听端口,默认 0.0.0.0:1036
listen=0.0.0.0:9090

#通过界面配置的信息保存文件夹,必须设置
data_dir=/data/bifrost/data

`````

##### 启动
`````shell
./Bifrost -config ./Bifrost.ini -d true

`````
##### 管理
http://127.0.0.1:9090


### 界面操作文档
[Bifrost官网](http://www.xbifrost.com/)

### QQ群号： 70331601

