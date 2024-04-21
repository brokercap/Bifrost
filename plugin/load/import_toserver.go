package init

import (
	_ "github.com/brokercap/Bifrost/plugin/ActiveMQ/src"
	_ "github.com/brokercap/Bifrost/plugin/Elasticsearch/src"
	_ "github.com/brokercap/Bifrost/plugin/MongoDB/src"
	_ "github.com/brokercap/Bifrost/plugin/TableCount/src"
	_ "github.com/brokercap/Bifrost/plugin/blackhole/src"
	_ "github.com/brokercap/Bifrost/plugin/clickhouse/src"
	_ "github.com/brokercap/Bifrost/plugin/hprose/src"
	_ "github.com/brokercap/Bifrost/plugin/http/src"
	_ "github.com/brokercap/Bifrost/plugin/kafka/src"
	_ "github.com/brokercap/Bifrost/plugin/memcache/src"
	_ "github.com/brokercap/Bifrost/plugin/mysql/src"
	_ "github.com/brokercap/Bifrost/plugin/rabbitmq/src"
	_ "github.com/brokercap/Bifrost/plugin/redis/src"
)
