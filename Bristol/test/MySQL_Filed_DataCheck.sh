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

pwd=123456

#dbType  mysql | mariadb
dbType=mysql
dbVerion=
rmDocker=true

if (( "$#" >= "2" )); then
    dbType=$1
    dbVerion=$2
else
    dbVerion=$1
fi

if (( "$#" >= "3" )); then
  rmDocker=$3
fi

echo dockerName $dockerName

if [ ! -n "$dbVerion" ]; then
  echo "\$1 is mysql version"
  exit 1
fi

echo server:$dbType
echo version:$dbVerion

function StartMySQLDocker() {
  echo "pwd:" $pwd
  version=$1
  sleepTime=10
  case "$version" in
    "latest")
      docker run -P --name $dockerName -e MYSQL_ROOT_PASSWORD=$pwd -e MYSQL_DATABASE=bifrost_test -e TZ=Asia/Shanghai -d mysql:$version --default-time_zone='+8:00' --skip-name-resolve --log-bin=/var/lib/mysql/mysql-bin.log --server-id=1 --binlog_format=ROW --sql-mode=ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION --default-authentication-plugin=mysql_native_password --gtid_mode=ON --enforce-gtid-consistency=true
    ;;

    "8"*)
      docker run -P --name $dockerName -e MYSQL_ROOT_PASSWORD=$pwd -e MYSQL_DATABASE=bifrost_test -e TZ=Asia/Shanghai -d mysql:$version --default-time_zone='+8:00' --skip-host-cache --skip-name-resolve --log-bin=/var/lib/mysql/mysql-bin.log --server-id=1 --binlog_format=ROW --sql-mode=ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION --default-authentication-plugin=mysql_native_password --gtid_mode=ON --enforce-gtid-consistency=true
    ;;

    "5.7"*)
      docker run -P --name $dockerName -e MYSQL_ROOT_PASSWORD=$pwd -e MYSQL_DATABASE=bifrost_test -e TZ=Asia/Shanghai -d mysql:$version --default-time_zone='+8:00' --skip-host-cache --skip-name-resolve --log-bin=/var/lib/mysql/mysql-bin.log --server-id=1 --binlog_format=ROW --sql-mode=ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,ERROR_FOR_DIVISION_BY_ZERO,NO_AUTO_CREATE_USER,NO_ENGINE_SUBSTITUTION --gtid_mode=ON --enforce-gtid-consistency=true
    ;;

    "5.6"*)
      docker run -P --name $dockerName -e MYSQL_ROOT_PASSWORD=$pwd -e MYSQL_DATABASE=bifrost_test -e TZ=Asia/Shanghai -d mysql:$version --default-time_zone='+8:00' --skip-host-cache --skip-name-resolve --log-bin=/var/lib/mysql/mysql-bin.log --server-id=1 --binlog_format=ROW --sql-mode=ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,ERROR_FOR_DIVISION_BY_ZERO,NO_AUTO_CREATE_USER,NO_ENGINE_SUBSTITUTION --default-storage-engine=MyISAM --loose-skip-innodb --default-tmp-storage-engine=MyISAM --gtid_mode=ON --enforce-gtid-consistency=true --log-bin --log-slave-updates
    ;;

    "5.5"*)
      docker run -P --name $dockerName -e MYSQL_ROOT_PASSWORD=$pwd -e MYSQL_DATABASE=bifrost_test -e TZ=Asia/Shanghai -d mysql:$version --default-time_zone='+8:00' --skip-host-cache --skip-name-resolve --log-bin=/var/lib/mysql/mysql-bin.log --server-id=1 --binlog_format=ROW --sql-mode=ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,ERROR_FOR_DIVISION_BY_ZERO,NO_AUTO_CREATE_USER,NO_ENGINE_SUBSTITUTION
    ;;

    "5.1"*)
      docker run -P --name $dockerName -e MYSQL_ROOT_PASSWORD=$pwd -e MYSQL_DATABASE=bifrost_test -e TZ=Asia/Shanghai -d jc3wish/mysql:5.1.73
    ;;

    *)
      echo "not suported mysql " $version
      exit 1
    ;;
  esac
  sleep $sleepTime
}

function StartPerconaDocker() {
  version=$1
  sleepTime=10
  case "$version" in
    "8"*)
      docker run -P --name $dockerName -e MYSQL_ROOT_PASSWORD=$pwd -e MYSQL_DATABASE=bifrost_test -e TZ=Asia/Shanghai -d percona:$version --default-time_zone='+8:00' --skip-host-cache --skip-name-resolve --log-bin=/var/lib/mysql/mysql-bin.log --server-id=1 --binlog_format=ROW --sql-mode=ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION --default-authentication-plugin=mysql_native_password --gtid_mode=ON --enforce-gtid-consistency=true
    ;;

    "5.7"*)
      docker run -P --name $dockerName -e MYSQL_ROOT_PASSWORD=$pwd -e MYSQL_DATABASE=bifrost_test -e TZ=Asia/Shanghai -d percona:$version --default-time_zone='+8:00' --skip-host-cache --skip-name-resolve --log-bin=/var/lib/mysql/mysql-bin.log --server-id=1 --binlog_format=ROW --sql-mode=ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,ERROR_FOR_DIVISION_BY_ZERO,NO_AUTO_CREATE_USER,NO_ENGINE_SUBSTITUTION --gtid_mode=ON --enforce-gtid-consistency=true
    ;;

    "5.6"*)
      docker run -P --name $dockerName -e MYSQL_ROOT_PASSWORD=$pwd -e MYSQL_DATABASE=bifrost_test -e TZ=Asia/Shanghai -d percona:$version --default-time_zone='+8:00' --skip-host-cache --skip-name-resolve --log-bin=/var/lib/mysql/mysql-bin.log --server-id=1 --binlog_format=ROW --sql-mode=ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,ERROR_FOR_DIVISION_BY_ZERO,NO_AUTO_CREATE_USER,NO_ENGINE_SUBSTITUTION --default-storage-engine=MyISAM --loose-skip-innodb --default-tmp-storage-engine=MyISAM --log-bin --log-slave-updates
    ;;

    "5.5"*)
      docker run -P --name $dockerName -e MYSQL_ROOT_PASSWORD=$pwd -e MYSQL_DATABASE=bifrost_test -e TZ=Asia/Shanghai -d percona:$version --default-time_zone='+8:00' --skip-host-cache --skip-name-resolve --log-bin=/var/lib/mysql/mysql-bin.log --server-id=1 --binlog_format=ROW --sql-mode=ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,ERROR_FOR_DIVISION_BY_ZERO,NO_AUTO_CREATE_USER,NO_ENGINE_SUBSTITUTION
    ;;

    *)
      echo "not suported percona " $version
      exit 1
    ;;
  esac
  sleep $sleepTime
}

function StartMariaDBDocker() {
  version=$1
  sleepTime=10
  docker run -P --name $dockerName -e MYSQL_ROOT_PASSWORD=$pwd -e MYSQL_DATABASE=bifrost_test -e TZ=Asia/Shanghai -d mariadb:$version --server_id=10 --default-time_zone='+8:00' --log_bin=mysql-bin --binlog_format=ROW
  sleep $sleepTime
}

case "$dbType" in
  "mysql")
    StartMySQLDocker $dbVerion
  ;;

  "mariadb")
    StartMariaDBDocker $dbVerion
  ;;

  "percona")
     StartPerconaDocker $dbVerion
  ;;

  *)
    echo "only supported mysql | mariadb | percona "
    exit 1
    ;;
esac

sleep 5

ip=
while ((i < 3))
do
    ip=`docker inspect --format '{{ .NetworkSettings.IPAddress }}' $dockerName`
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

#获取对应的映射端口
port=
port=`docker port $dockerName 3306`
port=${port#*:}
echo "docker -P" $port
sleep 15


$path/MySQL_Filed_DataCheck -u root -p $pwd -h 127.0.0.1 -P $port -database bifrost_test -table "" -longstring true 

if [ "$rmDocker" == "true" ]; then
docker stop $dockerName
docker rm $dockerName
fi
echo "over"