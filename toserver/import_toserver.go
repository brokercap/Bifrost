package toserver

import (
	_ "github.com/Bifrost/toserver/http"
	_ "github.com/Bifrost/toserver/mongodb"
	_ "github.com/Bifrost/toserver/rabbitmq"
	_ "github.com/Bifrost/toserver/memcache"
	_ "github.com/Bifrost/toserver/kafka"
	_ "github.com/Bifrost/toserver/activemq"
	_ "github.com/Bifrost/toserver/hprose"
	_ "github.com/Bifrost/toserver/redis"
)
