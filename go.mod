module github.com/brokercap/Bifrost

require (
	github.com/ClickHouse/clickhouse-go v1.4.3
	github.com/Shopify/sarama v1.29.0
	github.com/StackExchange/wmi v1.2.1
	github.com/agiledragon/gomonkey v2.0.2+incompatible
	github.com/bradfitz/gomemcache v0.0.0-20190329173943-551aad21a668
	github.com/gmallard/stompngo v1.0.11
	github.com/go-redis/redis/v8 v8.7.1
	github.com/hprose/hprose-golang v2.0.4+incompatible
	github.com/juju/errors v0.0.0-20200330140219-3fe23663418f
	github.com/juju/testing v0.0.0-20201216035041-2be42bba85f3 // indirect
	github.com/olivere/elastic/v7 v7.0.24
	github.com/rwynn/gtm/v2 v2.1.2
	github.com/satori/go.uuid v1.2.0
	github.com/smartystreets/goconvey v1.7.2
	github.com/streadway/amqp v0.0.0-20190404075320-75d898a42a94
	github.com/syndtr/goleveldb v1.0.0
	github.com/xdg/scram v1.0.5
	go.mongodb.org/mongo-driver v1.9.1
	golang.org/x/sys v0.0.0-20210112080510-489259a85091
	gopkg.in/mgo.v2 v2.0.0-20190816093944-a6b53ec6cb22

)

replace (
	golang.org/x/crypto => github.com/golang/crypto v0.0.0-20181001203147-e3636079e1a4
	golang.org/x/lint => github.com/golang/lint v0.0.0-20181026193005-c67002cb31c3
	golang.org/x/net => github.com/golang/net v0.0.0-20180826012351-8a410e7b638d
	golang.org/x/oauth2 => github.com/golang/oauth2 v0.0.0-20180821212333-d2e6202438be
	golang.org/x/sync => github.com/golang/sync v0.0.0-20181108010431-42b317875d0f
	golang.org/x/sys => github.com/golang/sys v0.0.0-20181116152217-5ac8a444bdc5
	golang.org/x/text => github.com/golang/text v0.3.0
	golang.org/x/time => github.com/golang/time v0.0.0-20180412165947-fbb02b2291d2
	golang.org/x/tools => github.com/golang/tools v0.0.0-20181219222714-6e267b5cc78e
	google.golang.org/api => github.com/googleapis/google-api-go-client v0.0.0-20181220000619-583d854617af
	google.golang.org/appengine => github.com/golang/appengine v1.3.0
	google.golang.org/grpc => github.com/grpc/grpc-go v1.17.0
	gopkg.in/alecthomas/kingpin.v2 => github.com/alecthomas/kingpin v2.2.6+incompatible
	gopkg.in/mgo.v2 => github.com/go-mgo/mgo v0.0.0-20180705113604-9856a29383ce
	gopkg.in/vmihailenco/msgpack.v2 => github.com/vmihailenco/msgpack v2.9.1+incompatible
	gopkg.in/yaml.v2 => github.com/go-yaml/yaml v0.0.0-20181115110504-51d6538a90f8
)
