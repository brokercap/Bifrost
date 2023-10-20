#/bin/bash

if [[ "$1" == "all" ]];then
  $0 linux amd64
  $0 linux arm64
  $0 windows amd64
  $0 darwin amd64
  $0 freebsd amd64
  $0 darwin arm64
  exit 0
fi

echo "假如下载依懒包慢,编译失败,请尝试修改 GOPROXY 代理"

echo "例如：export GOPROXY=https://goproxy.cn"
echo ""

function getInstallPmName()
{
    if grep -Eqii "CentOS" /etc/issue || grep -Eq "CentOS" /etc/*-release; then
        PM='yum'
    elif grep -Eqi "Red Hat Enterprise Linux Server" /etc/issue || grep -Eq "Red Hat Enterprise Linux Server" /etc/*-release; then
        PM='yum'
    elif grep -Eqi "Aliyun" /etc/issue || grep -Eq "Aliyun" /etc/*-release; then
        PM='yum'
    elif grep -Eqi "Fedora" /etc/issue || grep -Eq "Fedora" /etc/*-release; then
        PM='yum'
    elif grep -Eqi "Debian" /etc/issue || grep -Eq "Debian" /etc/*-release; then
        PM='apt'
    elif grep -Eqi "Ubuntu" /etc/issue || grep -Eq "Ubuntu" /etc/*-release; then
        DISTRO='Ubuntu'
        PM='apt'
    elif grep -Eqi "Raspbian" /etc/issue || grep -Eq "Raspbian" /etc/*-release; then
        PM='apt'
    else
        PM = ""
    fi
    echo $PM;
}

function installommand() {
    pm=$(getInstallPmName)
    case "$pm" in
      'yum')
        yum install -y $1
          ;;
      'apt')
        apt install -y $1
          ;;
       *)
          echo "install $1 please!!!"
          exit 1
          ;;
    esac
}

if type zip >/dev/null 2>&1; then
  echo ""
else
  installommand "zip"
fi

if type tar >/dev/null 2>&1; then
  echo ""
else
  installommand "tar"
fi


# 插件包的地址,:之后是版本号,假如本地调试的话,写上local,将会成$GOPATH里查找
PLUGINS=(
    #github.com/brokercap/bifrost_plugin_to_http:local
)

#其他依懒包,local代码本地包,将会成$GOPATH里加载
OTHERLAZY=(
    #github.com/brokercap/bifrost-core:local
)

### 方法简要说明：
### 1. 是先查找一个字符串：带双引号的key。如果没找到，则直接返回defaultValue。
### 2. 查找最近的冒号，找到后认为值的部分开始了，直到在层数上等于0时找到这3个字符：,}]。
### 3. 如果有多个同名key，则依次全部打印（不论层级，只按出现顺序）
###
### 4 params: json, key, defaultValue
function getJsonValuesByAwk() {
    awk -v json="$1" -v key="$2" -v defaultValue="$3" 'BEGIN{
        foundKeyCount = 0
        while (length(json) > 0) {
            # pos = index(json, "\""key"\""); ## 这行更快一些，但是如果有value是字符串，且刚好与要查找的key相同，会被误认为是key而导致值获取错误
            pos = match(json, "\""key"\"[ \\t]*?:[ \\t]*");
            if (pos == 0) {if (foundKeyCount == 0) {print defaultValue;} exit 0;}

            ++foundKeyCount;
            start = 0; stop = 0; layer = 0;
            for (i = pos + length(key) + 1; i <= length(json); ++i) {
                lastChar = substr(json, i - 1, 1)
                currChar = substr(json, i, 1)

                if (start <= 0) {
                    if (lastChar == ":") {
                        start = currChar == " " ? i + 1: i;
                        if (currChar == "{" || currChar == "[") {
                            layer = 1;
                        }
                    }
                } else {
                    if (currChar == "{" || currChar == "[") {
                        ++layer;
                    }
                    if (currChar == "}" || currChar == "]") {
                        --layer;
                    }
                    if ((currChar == "," || currChar == "}" || currChar == "]") && layer <= 0) {
                        stop = currChar == "," ? i : i + 1 + layer;
                        break;
                    }
                }
            }

            if (start <= 0 || stop <= 0 || start > length(json) || stop > length(json) || start >= stop) {
                if (foundKeyCount == 0) {print defaultValue;} exit 0;
            } else {
                print substr(json, start, stop - start);
            }

            json = substr(json, stop + 1, length(json) - stop)
        }
    }'
}

function showGoDownload(){
    echo ""
    echo "https://golang.org/dl/"
    echo ""
}

function checkGoVersion(){
    GoVersionResult=`go version`
    echo $GoVersionResult
    #$GoVersionResult:  go version go1.14.4 linux/amd64

    if [[ $GoVersionResult != *version* ]];then
        echo "go version error"
        #echo go version must go1.12+
        exit 1
    fi
    arr=($GoVersionResult)
    #arr:  (go version go1.14.4 linux/amd64)
    GoVersion0=${arr[2]}
    #GoVersion0:  go1.14.4
    GoVersion0=${GoVersion0:2}
    #GoVersion0: 1.14.4
    GoVersion1=${GoVersion0%%.*}
    #GoVersion1: 1
    GoVersion2=${GoVersion0#*.}
    #GoVersion2: 14.4
    GoVersion2=${GoVersion2%%.*}
    #GoVersion2: 14
    GoVersion3=$(( $GoVersion1*100+$GoVersion2*1 ))
    if [[ $GoVersion3 -lt 113 ]];then
        echo "go version must be go1.12+"
        exit 1
    fi
}

function checkOrDownloadGoEnv(){
   SYSTEM=`uname -s`
   if [ $SYSTEM = "Linux" ];then
        source /etc/profile
        GoVersionResult=`go version`
        if [[ $GoVersionResult != *version* ]];then
            # 假如 go 环境不存在则自动安装
            #echo "yum install -y golang    start"
            installommand "golang"
            echo "export GOROOT=/usr/lib/golang/" >> /etc/profile
            source /etc/profile
            #echo "yum install -y golang    over"
        fi
   fi
}

# # 兼容 travis 自动编译测试, travis 不需要自动判断是否存在 go 环境
# if [[ "$1" != "travis" ]];then
#   checkOrDownloadGoEnv
# fi

checkGoVersion

cd `dirname $0`

nowTime=$(date "+%Y%m%d%H%M%S")
#vendorBakDir=vendor.$nowTime

#cp ./go.mod ./go.mod.$vendorBakDir

#备份vendor，编译完了再拷备回来
#cp -r ./vendor $vendorBakDir

#生成依懒文件,将依懒包下载到vendor
init()
{
    if [[ ${#PLUGINS[*]} -eq 0 ]];then
        return
    fi
    importPluginFileName="./plugin/load/import_toserver2.go"

    echo "package plugin" > $importPluginFileName

    #将指定版本写到go.mod
    #也可以写成for element in ${array[*]}
    for element in ${PLUGINS[@]}
    do
        pluginVersion=${element#*:}

        echo $pluginVersion
        pluginDir=${element%%:*}

        echo "import " pluginDir":"$pluginVersion

        echo "import _ \"$pluginDir/www\"" >> $importPluginFileName
        echo "import _ \"$pluginDir/src\"" >> $importPluginFileName

        #==local 代表本地数据
        if [ "$pluginVersion" !=  "local" ]
        then
            echo "require ( " $pluginDir " ) " $pluginVersion >> ./go.mod
        fi
    done

    GO111MODULE=on go mod vendor

    #这里是将本地的包拷贝到vendor
    #也可以写成for element in ${array[*]}
    for element in ${PLUGINS[@]}
    do
        pluginVersion=${element#*:}

        echo $pluginVersion
        pluginDir=${element%%:*}

        #==local 代表本地数据
        if [ "$pluginVersion" ==  "local" ]
        then
            #假如GOPATH下存在指定包,则vendor下的删除掉
            if [ -d $GOPATH/src/$pluginDir ]
            then
                rm -rf ./vendor/$pluginDir
                mkdir -p ./vendor/$pluginDir
                pluginParentDir=`dirname $pluginDir`

                echo "copy " $GOPATH/src/$pluginDir " ==> " ./vendor/$pluginParentDir "starting"
                cp -rf $GOPATH/src/$pluginDir ./vendor/$pluginParentDir
                echo "copy " $GOPATH/src/$pluginDir " ==> " ./vendor/$pluginParentDir "over"

            fi
        fi
    done

}

copyLocalTtovVendor()
{
    #这里是将本地的包拷贝到vendor
    #也可以写成for element in ${array[*]}
    for element in ${OTHERLAZY[@]}
    do
        importSdkVersion=${element#*:}

        importSdkDir=${element%%:*}

        #==local 代表本地数据
        if [ "$importSdkVersion" ==  "local" ]
        then
            #假如GOPATH下存在指定包,则vendor下的删除掉
            if [ -d $GOPATH/src/$importSdkDir ]
            then
                rm -rf ./vendor/$importSdkDir
                mkdir -p ./vendor/$importSdkDir
                importSdkParentDir=`dirname $importSdkDir`

                echo "copy " $GOPATH/src/$importSdkDir " ==> " ./vendor/$importSdkParentDir "starting"
                cp -rf $GOPATH/src/$importSdkDir ./vendor/$importSdkParentDir
                echo "copy " $GOPATH/src/$importSdkDir " ==> " ./vendor/$importSdkParentDir "over"
            fi
        fi
    done
}

build()
{
    copyLocalTtovVendor

    mode=$1
    goarch=$2
    tagDir=$3
    bifrostVersion=$4

    echo "mkdir " $tagDir/admin
    echo "mkdir " $tagDir/plugin
    echo "mkdir " $tagDir/bin
    mkdir -p $tagDir/admin
    mkdir -p $tagDir/plugin
    mkdir -p $tagDir/bin

    echo "$mode build starting "
    if [[ $goarch == "none" ]];then
        goarch=amd64
    fi
    CGO_ENABLED=0 GOOS=$mode GOARCH=$goarch go build -gcflags=-trimpath=$GOPATH -asmflags=-trimpath=$GOPATH -ldflags "-w -s" ./Bifrost.go

    echo "$mode build over "

    if [[ "$mode" == "windows" ]];then
        if [ ! -f "./Bifrost.exe" ]; then
            echo "build error"
            exit 1
        fi
        mv Bifrost.exe ./$tagDir/bin
        cp -f ./Bifrost-server.bat ./$tagDir/bin
    else
        if [ ! -f "./Bifrost" ]; then
            echo "build error"
            exit 1
        fi
        echo "copy ./Bifrost ==> " ./$tagDir/bin
        echo "copy ./Bifrost-server ==> " ./$tagDir/bin
        mv ./Bifrost ./$tagDir/bin
        cp -f ./Bifrost-server ./$tagDir/bin
    fi

    echo $bifrostVersion > $tagDir/VERSION
    cp -rf ./README.MD ./$tagDir/README.MD
    cp -rf ./LICENSE ./$tagDir/LICENSE
    cp -rf ./go.mod ./$tagDir/go.mod
    cp -rf ./go.sum ./$tagDir/go.sum
    
    echo "copy ./admin/view ==> " ./$tagDir/admin/view

    cp -rf ./admin/view ./$tagDir/admin/view

    echo "copy ./etc ==> " ./$tagDir/etc
    cp -r ./etc ./$tagDir/

    echo "copy ./免责声明.txt ==> " ./$tagDir/
    cp -rf ./免责声明.txt ./$tagDir/免责声明.txt

    #拷贝./plugin/import_toserver.go 中加载了的默认插件到编译之后的tags目录下
    import_toserver_content=`cat ./plugin/load/import_toserver.go`
    #echo $import_toserver_content
    for element in `ls ./plugin`
    do
        localPluginDir="./plugin/"$element
        if [ -d $dir_or_file ]
        then
            if [ -d $localPluginDir/www ]
            then
                #只有在./plugin/import_toserver.go 加载了插件,才可以被拷贝 www 等信息到编译目录
                if [[ ! "${import_toserver_content}" =~ "${element}" ]];then
                    #echo  "${element}"
                    continue
                fi
                config_file=$localPluginDir/www/config.json
                if [ -f "$config_file" ]
                then
                    json=`cat $config_file`
                    pluginNameString=( $( getJsonValuesByAwk "$json" "name" "$pluginName0" ) )
                    pluginNameStringL=${#pluginNameString}
                    pluginName=${pluginNameString:1:pluginNameStringL-2}
                else
                    pluginName=$element
                fi

                mkdir -p $tagDir/plugin/$pluginName
                echo $tagDir/plugin/$pluginName

                echo "copy "  $localPluginDir/www " ==> " $tagDir/plugin/$pluginName/
                cp -rf $localPluginDir/www $tagDir/plugin/$pluginName/

            fi
        fi
    done

    for element in ${PLUGINS[@]}
    do
        pluginVersion=${element#*:}
        pluginDir=${element%%:*}
        localPluginDir=
        if [[ $pluginDir == github.com/brokercap/Bifrost* ]];then
            localPluginDir=${pluginDir:29}
        else
            if [ -d "./vendor/"$pluginDir ]
            then
                localPluginDir="./vendor/"$pluginDir
            else
                 localPluginDir=$GOPATH/src/$pluginDir
            fi
        fi
        
        if [ -d $localPluginDir ]
        then
            if [ -d $localPluginDir/www ]
            then
                echo $localPluginDir
                config_file=$localPluginDir/www/config.json
                if [ -f "$config_file" ]
                then
                    json=`cat $config_file`
                    pluginNameString=( $( getJsonValuesByAwk "$json" "name" "$pluginName0" ) )
                    pluginNameStringL=${#pluginNameString}
                    pluginName=${pluginNameString:1:pluginNameStringL-2}
                else
                    pluginName=$element
                fi
                mkdir -p $tagDir/plugin/$pluginName
                echo $tagDir/plugin/$pluginName

                echo "copy "  $localPluginDir/www " ==> " $tagDir/plugin/$pluginName/
                cp -rf $localPluginDir/www $tagDir/plugin/$pluginName/
            fi
        fi
    done

    tagDirName="${tagDir##*/}"
    cd $tagDir && cd ../
    case "$mode" in
        'windows')
        zip -r "$tagDirName".zip ./$tagDirName
        ;;
       *)
        tar -czvf "$tagDirName".tar.gz ./$tagDirName
        ;;
    esac
    echo "build over"
}

function buildHelp(){
    echo " golang version 1.13+ need "
    echo ""
    echo "./build.sh init"
    echo "--- go mod vendor"
    echo "./build.sh linux|windows|freebsd|darwin"
    echo "--- build for linux|windows|freebsd|darwin"
    echo "./build.sh install ./targetdir linux"
    echo "--- build for linux ,and target is ./targetdir "
    echo "./build.sh clean"
    echo "--- clean build cache "
}


#clean
if [[ "$1" == "clean" ]];then
    rm -rf tags/$BifrostVersion
    exit 0
fi

#clean
if [[ "$1" == "help" ]];then
    buildHelp
    exit 0
fi


mode=$(echo $1 | tr '[A-Z]' '[a-z]')

if [[ "$mode" == "init" ]];then
    init
    echo "init over"
    exit 0
fi

BifrostVersion=`cat ./config/version.go | awk -F'=' '{print $2}' | sed 's/"//g' | tr '\n' ' ' | sed s/[[:space:]]//g`


GOARCH="none"
if [[ "$1" == "install" ]];then
    if [[ "$3" != "" ]];then
        mode=$3
    fi
    if [[ "$4" != "" ]];then
        GOARCH=$4
    fi
fi

#./build install $target linux amd64
if [[ "$1" == "" || "$1" == "travis" || (( "$1" == "install" && "$3" == "" )) ]];then
   SYSTEM=`uname -s`
   if [ $SYSTEM = "Linux" ];then
       mode="linux"
   elif [ $SYSTEM = "FreeBSD" ];then
       mode="freebsd"
   elif [ $SYSTEM = "Darwin" ];then
       mode="darwin"
   else
       echo "cant't support $SYSTEM"
       exit 1
   fi
fi
ModeName=
case "$mode" in
    'windows')
      ModeName="Win"
        ;;
    'linux')
      ModeName="Linux"
        ;;
    'darwin')
      ModeName="Darwin"
        ;;
    'freebsd')
      ModeName="FreeBSD"
        ;;
     *)
        echo "cant't support $mode"
        exit 1
        ;;
esac


#./build install ./targetdir linux amd64
if [[ "$1" == "install" ]];then
    if [[ "$2" == "" ]];then
        echo "prefix dir is empty"
        exit 1
    fi
    mkdir -p $2
    build $mode $GOARCH $2 $BifrostVersion
    exit 0
fi

if [[ $GOARCH == "none" || $GOARCH == "" ]];then
    if [[ $2 != "" ]]; then
        GOARCH=$2
    else
        # go version go1.14.4 linux/amd64
        # 获取 amd64
        GoVersionResult=`go version`
        GOARCH=${GoVersionResult#*\/}
    fi
fi


if [ ! -n "$BifrostVersion" ] ;then
    tagDir=tags/bifrost_$BifrostVersion_$ModeName-$GOARCH-bin
else
    tagDir=tags/$BifrostVersion/bifrost_"$BifrostVersion"_$ModeName-$GOARCH-bin
fi

rm -rf $tagDir0
build $mode $GOARCH $tagDir $BifrostVersion

echo "target:" $tagDir
echo ""
#mv -rf $vendorBakDir ./vendor