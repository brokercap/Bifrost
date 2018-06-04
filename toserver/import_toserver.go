package toserver

import (
	_ "github.com/jc3wish/Bifrost/toserver/http"
	_ "github.com/jc3wish/Bifrost/toserver/mongodb"
	_ "github.com/jc3wish/Bifrost/toserver/rabbitmq"
	_ "github.com/jc3wish/Bifrost/toserver/memcache"
	_ "github.com/jc3wish/Bifrost/toserver/kafka"
	_ "github.com/jc3wish/Bifrost/toserver/activemq"
	_ "github.com/jc3wish/Bifrost/toserver/hprose"
	_ "github.com/jc3wish/Bifrost/toserver/redis"
)
