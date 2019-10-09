#/bin/bash
dockerDevVersion=devTest

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
    dockerCleanDev
    mkdir ./BifrostDevTestData
    docker run --name BifrostDevTest -d -P -v BifrostDevTestData:/linux/data jc3wish/bifrost:$dockerDevVersion
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
    'release_build')
        if [[ "$2" == "" ]];then
            sys=centos
        else
            sys=$2
        fi
        dockerBuildRelease $sys
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