#/bin/bash
cd `dirname $0`
mode=$1

if [ ! -n "$2" ] ;then
tagDir=tags/$1
else
tagDir=tags/$2/$1
fi
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

mkdir -p $tagDir/manager
mkdir -p $tagDir/plugin
mkdir -p $tagDir/etc


CGO_ENABLED=0 GOOS=$mode GOARCH=amd64 go build ./Bifrost.go

if [[ "$1" == "windows" ]];then
    mv Bifrost.exe ./$tagDir
else
    mv ./Bifrost ./$tagDir
    cp -f ./Bifrost-server ./$tagDir
fi

cp -rf ./manager/template ./$tagDir/manager/template
cp -rf ./manager/public ./$tagDir/manager/public
cp -f ./etc/Bifrost.ini ./$tagDir/etc
for element in `ls ./plugin`
do
    dir_or_file="./plugin/"$element
    if [ -d $dir_or_file ]
    then
        if [ -d $dir_or_file/www ]
        then
            mkdir -p $tagDir/plugin/$element
            cp -rf $dir_or_file/www $tagDir/plugin/$element/
        fi
    fi
done

echo "$1 build over"

