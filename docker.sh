#/bin/bash
dockerDevVersion=devTest
dockerGoVersionTest=bifrostGoVersionTest

function dockerBuildDev(){
    sys=$1
    ./build.sh install Dockerfile/$sys/linux linux
    docker build -f ./Dockerfile/$sys/Dockerfile -t jc3wish/bifrost:$dockerDevVersion ./Dockerfile/$sys
    rm -rf Dockerfile/$sys/linux
    echo "build jc3wish/bifrost:$dockerDevVersion over"
}


function dockerCleanDev(){
    dockerStopDev
    docker rmi jc3wish/bifrost:$dockerDevVersion
    echo "docker rmi jc3wish/bifrost:$dockerDevVersion success "
}

function dockerStopDev(){
    docker stop  BifrostDevTest
    echo "stop BifrostDevTest success"
    docker rm  BifrostDevTest
    echo "rm BifrostDevTest success"
}

function dockerRunDev(){
    dockerStopDev
    mkdir ./BifrostDevTestData
    docker run --name BifrostDevTest -d -P -v BifrostDevTestData:/linux/data jc3wish/bifrost:$dockerDevVersion
    docker container port BifrostDevTest
}

function dockerBuildByGoVersion(){
    if [[ "$1" == "" ]];then
        version="latest"
    else
        version=$1
    fi
    mkdir -p Dockerfile/go{$version}
    echo "FROM golang:$version
    MAINTAINER jc3wish 'jc3wish@126.com'
    RUN cd /bin && rm -f sh && ln -s /bin/bash sh
    RUN mkdir -p ./Bifrost-server
    COPY ./ ./Bifrost-server/
    RUN cd ./Bifrost-server && chmod a+x ./build.sh && ./build.sh install /usr/local/Bifrost-server && rm -rf ./Bifrost-server && chmod a+x /usr/local/Bifrost-server/Bifrost* && make -p /usr/local/Bifrost-server/data
    ENTRYPOINT ['/usr/local/Bifrost-server/Bifrost-server','start']
    EXPOSE 21036
    " > Dockerfile/go{$version}/Dockerfile
    docker build -f Dockerfile/go{$version}/Dockerfile -t jc3wish/bifrost:$dockerGoVersionTest .
}

function dockerBuildTest(){
    dockerBuildByGoVersion $1
}

function dockerCleanTest(){
    dockerStopTest
    docker rmi jc3wish/bifrost:$dockerGoVersionTest
    echo "docker rmi jc3wish/bifrost:$dockerDevVersion success "
}

function dockerStopTest(){
    docker stop  BifrostGoVersionTest
    echo "stop BifrostGoVersionTest success"
    docker rm  BifrostGoVersionTest
    echo "rm BifrostGoVersionTest success"
}

function dockerRunTest(){
    dockerStopTest
    mkdir ./BifrostDevTestData
    docker run --name BifrostGoVersionTest -d -P -v BifrostDevTestData:/usr/local/Bifrost-server/data jc3wish/bifrost:$dockerGoVersionTest
    docker container port BifrostDevTest
}


function dockerBuildRelease(){
    sys=$1
    ./build.sh install Dockerfile/$sys/linux linux
    dockerVersion=`cat Dockerfile/$sys/linux/VERSION`
    docker build --file ./Dockerfile/$sys/Dockerfile -t jc3wish/bifrost:$dockerVersion ./Dockerfile/$sys
    rm -rf Dockerfile/$sys/linux
    echo "build jc3wish/bifrost:$dockerVersion over"
}

function dockerOnlineTestAdmin(){
    docker stop BifrostOnlineTest
    docker rm BifrostOnlineTest
    mkdir -p /data/BifrostOnlineTestData
    if [[ "$1" == "" ]];then
        dockerVersion=`cat ./config/version.go | awk -F'=' '{print $2}' | sed 's/"//g' | tr '\n' ' ' | sed s/[[:space:]]//g`
        v=jc3wish/bifrost:$dockerVersion
    else
        v=$1
    fi
    rm -f /data/BifrostOnlineTestData/data/*.pid
    docker run --name BifrostOnlineTest -d -p21039:21036 -v /data/BifrostOnlineTestData/data:/linux/data -v /data/BifrostOnlineTestData/logs:/linux/logs -v /data/BifrostOnlineTestData/etc:/linux/etc $v
}

function dockerCleanRelease(){
    dockerVersion=`cat ./config/version.go | awk -F'=' '{print $2}' | sed 's/"//g' | tr '\n' ' ' | sed s/[[:space:]]//g`
    docker rmi jc3wish/bifrost:$dockerVersion
    echo "rmi jc3wish/bifrost:$dockerVersion over"
}

function dockerPushRelease(){
    if [[ "$1" == "" ]];then
        dockerVersion=`cat ./config/version.go | awk -F'=' '{print $2}' | sed 's/"//g' | tr '\n' ' ' | sed s/[[:space:]]//g`
        v=jc3wish/bifrost:$dockerVersion
    else
        v=$1
    fi
    docker push $v
    
    docker tag $v jc3wish/bifrost:latest
    
    docker push jc3wish/bifrost:latest
}

function dockerClean(){
    docker ps -a | grep Exited | awk '{print $1}'|xargs docker rm
    docker images|grep none|awk '{print $3}'|xargs docker rmi
}

function getIp(){
    docker inspect --format '{{ .NetworkSettings.IPAddress }}' $1
}

function dockerStartAll(){
    docker start $(docker ps -a | awk '{ print $1}' | tail -n +2)
}

function dockerRunToServer(){
	#activemq
	docker run -d --name activemq -p 61616:61616 -p 8161:8161 docker.io/webcenter/activemq:latest
	
	#redis
	docker run -p 6379:6379 -d redis:3.0
	#docker run -it --rm --link some-clickhouse-server:clickhouse-server yandex/clickhouse-client --host clickhouse-server
	
	#mysql
	docker run --name mysql8 -e MYSQL_ROOT_PASSWORD=root -d mysql:8.0 --skip-host-cache --skip-name-resolve --log-bin=/var/lib/mysql/mysql-bin.log --server-id=1 --binlog_format=ROW --default-authentication-plugin=mysql_native_password --sql-mode=ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTIO
	#kakfa
	docker run -d --name zookeeper -p 2181:2181 -t wurstmeister/zookeeper 
	docker run -d --name kafka --publish 9092:9092 --link zookeeper --env KAFKA_ZOOKEEPER_CONNECT=zookeeper:2181 --env KAFKA_ADVERTISED_HOST_NAME=localhost --env KAFKA_ADVERTISED_PORT=9092 --volume /etc/localtime:/etc/localtime wurstmeister/kafka:latest
	
	#rabbitmq
	docker run -dit --name Myrabbitmq -e RABBITMQ_DEFAULT_USER=admin -e RABBITMQ_DEFAULT_PASS=admin -p 15672:15672 -p 5672:5672 rabbitmq:managemen
	
	#memcache
	docker run -p 11211:11211 --name memcache memcached
}

function dockerHelp(){
    echo "dev_build"
    echo "dev_clean"
    echo "dev_run"
    echo "dev_stop"
    echo "test_build [golang version 1.13+]"
    echo "test_clean"
    echo "test_run"
    echo "test_stop"
    echo "release_build"
    echo "push"
    echo "clean    -- clean all exit and none images"
    echo "online_test_run jc3wish/bifrost:v1.6.0   -- clean all exit and none images"
    echo "getip dockerName "
    echo "start_all "
}

case "$1" in
    'dev_build')
        if [[ "$2" == "" ]];then
            sys=centos
        else
            sys=$2
        fi
        dockerBuildDev $sys
        ;;
    'dev_clean')
        dockerCleanDev
        ;;
    'dev_run')
        dockerRunDev
        ;;
    'dev_stop')
        dockerStopDev
        ;;
        
    'test_build')
        dockerBuildTest $2
        ;;
    'test_clean')
        dockerCleanTest
        ;;
    'test_run')
        dockerRunTest
        ;;
    'test_stop')
        dockerStopTest
        ;;
    'release_build')
        if [[ "$2" == "" ]];then
            sys=centos
        else
            sys=$2
        fi
        dockerBuildRelease $sys
        ;;
     'release_clean')
        dockerCleanRelease
        ;;
    'push')
        dockerPushRelease
        ;;
     'online_test_run')
	      dockerOnlineTestAdmin $2
	      ;;
     'clean')
        dockerClean
        ;;
     'getip')
        getIp $2
        ;;
     'start_all')
        dockerStartAll
        ;;
     *)
        dockerHelp
        ;;
esac
