## Bifrost ---- MySQL到其他服务的高性能异构中间件

### 简单  高效

### QQ群号： 70331601

#### 编译
`````shell
# windows
./build.sh windows

# linux
./build.sh linux

#mac
./build.sh darwin

#freebsd
./build.sh freebsd

`````


#### 配置
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

#### 启动
`````shell
./Bifrost -config ./Bifrost.ini

`````



