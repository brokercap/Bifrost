#!/bin/bash

function randstr() {
  index=0
  str=""
  for i in {a..z}; do arr[index]=$i; index=`expr ${index} + 1`; done
  for i in {A..Z}; do arr[index]=$i; index=`expr ${index} + 1`; done
  for i in {0..9}; do arr[index]=$i; index=`expr ${index} + 1`; done
  for i in {1..20}; do str="$str${arr[$RANDOM%$index]}"; done
  echo $str
}

# 在$()的圆括号中可以执行linux命令,当然也包括执行函数
dockerName=$(randstr)
path=$(dirname $0)
cd ./${path}  # 当前位置跳到脚本位置
path=$(pwd)   # 取到脚本目录 

echo dockerName $dockerName
version=$1

if [ ! -n "$version" ]; then
  echo "\$1 is mysql version"
  exit 1
fi

echo mysqlVerion:$version

pwd=123456

if [ "${version:0:1}" = "8" ];then
/usr/bin/docker run --name $dockerName -e MYSQL_ROOT_PASSWORD=$pwd -e MYSQL_DATABASE=bifrost_test -e TZ=Asia/Shanghai -d mysql:$version --default-time_zone='+8:00' --skip-host-cache --skip-name-resolve --log-bin=/var/lib/mysql/mysql-bin.log --server-id=1 --binlog_format=ROW --default-authentication-plugin=mysql_native_password --sql-mode=ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION
sleep 8

else

tmp=${version:0:3}

if [ "$tmp" = "5.6" ];then
  /usr/bin/docker run --name $dockerName -e MYSQL_ROOT_PASSWORD=$pwd -e MYSQL_DATABASE=bifrost_test -e TZ=Asia/Shanghai -d mysql:$version --default-time_zone='+8:00' --skip-host-cache --skip-name-resolve --log-bin=/var/lib/mysql/mysql-bin.log --server-id=1 --binlog_format=ROW --sql-mode=ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,ERROR_FOR_DIVISION_BY_ZERO,NO_AUTO_CREATE_USER,NO_ENGINE_SUBSTITUTION --default-storage-engine=MyISAM --loose-skip-innodb --default-tmp-storage-engine=MyISAM

echo "5.6 test"

elif [ "$tmp" = "5.1" ]; then
  /usr/bin/docker run -d --name $dockerName -e MYSQL_ROOT_PASSWORD=$pwd -e MYSQL_DATABASE=bifrost_test -e TZ=Asia/Shanghai jc3wish/mysql:5.1.73
  sleep 10
else
  /usr/bin/docker run --name $dockerName -e MYSQL_ROOT_PASSWORD=$pwd -e MYSQL_DATABASE=bifrost_test -e TZ=Asia/Shanghai -d mysql:$version --default-time_zone='+8:00' --skip-host-cache --skip-name-resolve --log-bin=/var/lib/mysql/mysql-bin.log --server-id=1 --binlog_format=ROW --sql-mode=ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,ERROR_FOR_DIVISION_BY_ZERO,NO_AUTO_CREATE_USER,NO_ENGINE_SUBSTITUTION
fi

sleep 5

fi


ip=
while ((i < 3))
do
    ip=`/usr/bin/docker inspect --format '{{ .NetworkSettings.IPAddress }}' $dockerName`
    if [ -n "$ip" ]; then
      break
    fi
    ((i++))
    sleep 2
done


if [ ! -n "$ip" ]; then
  echo $dockerName ip is emtpy
  exit 1
fi


echo $ip

sleep 15


$path/MySQL_Filed_DataCheck -u root -p $pwd -h $ip -database bifrost_test -table "" -longstring false


/usr/bin/docker stop $dockerName

/usr/bin/docker rm $dockerName

echo "over"