#/bin/bash
cd `dirname $0`
mode=$1
case "$mode" in
    'windows')
        ;;
    'linux')
        ;;
    'darwin')
        ;;
    'freebsd')
        ;;
    *)
        $mode = "linux"
        ;;
esac

mkdir -p $1/manager

CGO_ENABLED=0 GOOS=$mode GOARCH=amd64 go build ./Bifrost.go

if [[ "$1" == "windows" ]];then
    mv Bifrost.exe ./windows
    cp -rf ./manager/template ./windows/manager/template
    cp -rf ./manager/public ./windows/manager/public
    cp -f ./Bifrost.ini ./windows
else
    mv ./Bifrost ./$1
    cp -rf ./manager/template ./$1/manager/template
    cp -rf ./manager/public ./$1/manager/public
    cp -f ./Bifrost.ini ./$1
fi

echo "$1 build over"

