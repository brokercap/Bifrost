basepath=$(cd `dirname $0`;pwd)
TlsDir=$basepath/tls
mkdir -p $TlsDir
PASSWORD=123456

function CreateTLS(){

  openssl genrsa -out $TlsDir/ca-key.pem 4096 -passout pass:$PASSWORD
  openssl req -new -sha256 -out $TlsDir/ca-cs.pem -key $TlsDir/ca-key.pem -config ./conf/ca.conf -passin pass:$PASSWORD
  openssl x509 -req -days 3650 -in $TlsDir/ca-cs.pem -signkey $TlsDir/ca-key.pem -out $TlsDir/ca-crt.pem

  openssl genrsa -out $TlsDir/server-key.pem 4096 -passout pass:$PASSWORD
  openssl req -new -sha256 -out $TlsDir/server-cs.pem -key $TlsDir/server-key.pem -config ./conf/server.conf -passin pass:$PASSWORD
  openssl x509 -req -days 3650 -CA $TlsDir/ca-crt.pem -CAkey $TlsDir/ca-key.pem -CAcreateserial -in $TlsDir/server-cs.pem -out $TlsDir/server-cert.pem -extensions req_ext -extfile ./conf/server.conf

  openssl genrsa -out $TlsDir/client-key.pem 4096 -passout pass:$PASSWORD
  openssl req -new -sha256 -out $TlsDir/client-cs.pem -key $TlsDir/client-key.pem -config ./conf/client.conf -passin pass:$PASSWORD
  openssl x509 -req -days 3650 -CA $TlsDir/ca-crt.pem -CAkey $TlsDir/ca-key.pem -CAcreateserial -in $TlsDir/client-cs.pem -out $TlsDir/client-cert.pem -extensions req_ext -extfile ./conf/client.conf
}

function StartDocker(){
    echo "docker run"
    docker run -p 53306:3306 --name mysqlTLS -v $basepath/tls:/etc/mysql/tls -v $basepath/my.cnf:/etc/my.cnf -e MYSQL_ROOT_PASSWORD=$PASSWORD -e MYSQL_DATABASE=bifrost_test -e TZ=Asia/Shanghai -d mysql:latest --default-time_zone='+8:00' --skip-name-resolve --log-bin=/var/lib/mysql/mysql-bin.log --server-id=1 --binlog_format=ROW --sql-mode=ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION --default-authentication-plugin=mysql_native_password --gtid_mode=ON --enforce-gtid-consistency=true
    sleep 5
}

function StartGoTest() {
    echo "go test -v -tags integration ./"
    go test -v -tags integration ./
}

function RmDocker() {
    echo "docker stop mysqlTLS"
    docker stop mysqlTLS
    echo "docker rm mysqlTLS"
    docker rm mysqlTLS
}

case "$1" in
  'tls')
    CreateTLS
    exit 0
    ;;

  'docker')
    StartDocker
    exit 0
    ;;

  'test')
    StartGoTest
    exit 0
    ;;

  'rmdocker')
    RmDocker
    exit 0
    ;;

  'all')
    CreateTLS
    StartDocker
    sleep 10
    StartGoTest
    RmDocker
    exit 0
    ;;
  *)
    # usage
    basename=`basename "$0"`
    echo "Usage: $basename  {tls|docker|test|rmdocker|all}  [ start.sh options ]"
    exit 1
  ;;
esac