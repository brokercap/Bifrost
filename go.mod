module github.com/brokercap/Bifrost

go 1.24.0

require (
	github.com/ClickHouse/clickhouse-go v1.4.3
	github.com/SAP/go-hdb v1.14.18
	github.com/Shopify/sarama v1.29.0
	github.com/agiledragon/gomonkey/v2 v2.11.0
	github.com/bradfitz/gomemcache v0.0.0-20190329173943-551aad21a668
	github.com/gmallard/stompngo v1.0.11
	github.com/go-redis/redis/v8 v8.7.1
	github.com/hprose/hprose-golang v2.0.4+incompatible
	github.com/juju/errors v0.0.0-20200330140219-3fe23663418f
	github.com/olivere/elastic/v7 v7.0.24
	github.com/robfig/cron/v3 v3.0.1
	github.com/rwynn/gtm/v2 v2.1.2
	github.com/satori/go.uuid v1.2.0
	github.com/smartystreets/goconvey v1.7.2
	github.com/streadway/amqp v0.0.0-20190404075320-75d898a42a94
	github.com/syndtr/goleveldb v1.0.0
	github.com/xdg/scram v1.0.5
	go.mongodb.org/mongo-driver v1.17.2
	gopkg.in/mgo.v2 v2.0.0-20190816093944-a6b53ec6cb22
)

require (
	github.com/cespare/xxhash/v2 v2.1.1 // indirect
	github.com/cloudflare/golz4 v0.0.0-20150217214814-ef862a3cdc58 // indirect
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/dgryski/go-rendezvous v0.0.0-20200823014737-9f7001d12a5f // indirect
	github.com/eapache/go-resiliency v1.2.0 // indirect
	github.com/eapache/go-xerial-snappy v0.0.0-20180814174437-776d5712da21 // indirect
	github.com/eapache/queue v1.1.0 // indirect
	github.com/golang/snappy v0.0.4 // indirect
	github.com/gopherjs/gopherjs v0.0.0-20181017120253-0766667cb4d1 // indirect
	github.com/hashicorp/go-uuid v1.0.2 // indirect
	github.com/jcmturner/aescts/v2 v2.0.0 // indirect
	github.com/jcmturner/dnsutils/v2 v2.0.0 // indirect
	github.com/jcmturner/gofork v1.0.0 // indirect
	github.com/jcmturner/gokrb5/v8 v8.4.2 // indirect
	github.com/jcmturner/rpc/v2 v2.0.3 // indirect
	github.com/josharian/intern v1.0.0 // indirect
	github.com/jtolds/gls v4.20.0+incompatible // indirect
	github.com/juju/testing v0.0.0-20201216035041-2be42bba85f3 // indirect
	github.com/klauspost/compress v1.16.7 // indirect
	github.com/mailru/easyjson v0.7.7 // indirect
	github.com/montanaflynn/stats v0.7.1 // indirect
	github.com/pierrec/lz4 v2.6.0+incompatible // indirect
	github.com/pkg/errors v0.9.1 // indirect
	github.com/rcrowley/go-metrics v0.0.0-20201227073835-cf1acfcdf475 // indirect
	github.com/serialx/hashring v0.0.0-20200727003509-22c0c7ab6b1b // indirect
	github.com/smartystreets/assertions v1.2.0 // indirect
	github.com/xdg-go/pbkdf2 v1.0.0 // indirect
	github.com/xdg-go/scram v1.1.2 // indirect
	github.com/xdg-go/stringprep v1.0.4 // indirect
	github.com/xdg/stringprep v1.0.3 // indirect
	github.com/youmark/pkcs8 v0.0.0-20240726163527-a2c0da244d78 // indirect
	go.opentelemetry.io/otel v0.18.0 // indirect
	go.opentelemetry.io/otel/metric v0.18.0 // indirect
	go.opentelemetry.io/otel/trace v0.18.0 // indirect
	golang.org/x/crypto v0.26.0 // indirect
	golang.org/x/net v0.0.0-20210427231257-85d9c07bbe3a // indirect
	golang.org/x/sync v0.8.0 // indirect
	golang.org/x/text v0.33.0 // indirect
)

replace (
	github.com/rwynn/gtm/v2 => github.com/jc3wish/gtm/v2 v2.0.0-20250316050549-7984c507086d
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
