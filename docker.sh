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
    docker build -t --file ./Dockerfile/$sys/Dockerfile jc3wish/bifrost:$dockerVersion ./Dockerfile/$sys
    rm -rf Dockerfile/$sys/linux
    echo "build jc3wish/bifrost:$dockerVersion over"
}

function dockerCleanRelease(){
    dockerVersion=`cat ./config/version.go | awk -F'=' '{print $2}' | sed 's/"//g' | tr '\n' ' ' | sed s/[[:space:]]//g`
    docker rmi jc3wish/bifrost:$dockerDevVersion
    echo "rmi jc3wish/bifrost:$dockerVersion over"
}

function dockerPushRelease(){
    docker push $1
}

function dockerClean(){
    docker ps -a | grep Exited | awk '{print $1}'|xargs docker rm
    docker images|grep none|awk '{print $3}'|xargs docker rmi
}

function dockerHelp(){
    echo "dev_build"
    echo "dev_clean"
    echo "dev_run"
    echo "dev_stop"
    echo "test_build [1.12|1.13(golang version)]"
    echo "test_clean"
    echo "test_run"
    echo "test_stop"
    echo "release_build"
    echo "push"
    echo "clean    -- clean all exit and none images"
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
     'clean')
        dockerClean
        ;;
     *)
        dockerHelp
        ;;
esac
