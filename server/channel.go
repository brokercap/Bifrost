
/*
Copyright [2018] [jc3wish]

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
package server

import (
	"sync"

	"github.com/brokercap/Bifrost/Bristol/mysql"
	"github.com/brokercap/Bifrost/server/count"
	"github.com/brokercap/Bifrost/config"
	"log"
)

type Channel struct {
	sync.RWMutex
	Name string
	chanName         chan mysql.EventReslut
	MaxThreadNum     int // 消费通道的最大线程数
	CurrentThreadNum int
	Status           string //stop ,starting,running,wait
	db               *db
	countChan		 chan  *count.FlowCount
}

func NewChannel(MaxThreadNum int,Name string, db *db) *Channel {
	return &Channel{
		Name:					Name,
		chanName:             	make(chan mysql.EventReslut, MaxThreadNum*config.ChannelQueueSize),
		MaxThreadNum:     		MaxThreadNum,
		CurrentThreadNum: 		0,
		Status:           		"stop",
		db:               		db,
	}
}

func GetChannel(name string,channelID int) *Channel{
	if _,ok:=DbList[name];!ok{
		return nil
	}
	DbList[name].Lock()
	defer  DbList[name].Unlock()
	if _,ok:=DbList[name].channelMap[channelID];!ok{
		return nil
	}
	return DbList[name].channelMap[channelID]
}

func DelChannel(name string,channelID int) bool{
	if _,ok:=DbList[name];!ok{
		return false
	}
	if _,ok:=DbList[name].channelMap[channelID];!ok{
		return false
	}
	log.Println(DbList[name].Name,"Channel:",DbList[name].channelMap[channelID].Name,"delete")
	delete(DbList[name].channelMap,channelID)
	return true
}

func (Channel *Channel) SetFlowCountChan(flowChan chan *count.FlowCount) {
	Channel.countChan = flowChan
}

func (Channel *Channel) GetCountChan() chan *count.FlowCount {
	return Channel.countChan
}

func (Channel *Channel) Start() chan mysql.EventReslut {
	Channel.Lock()
	defer Channel.Unlock()
	log.Println(Channel.db.Name,"Channel:",Channel.Name,"start")
	if Channel.Status == "running"{
		return Channel.chanName
	}
	Channel.Status = "running"
	for i := 0; i < Channel.MaxThreadNum; i++ {
		go Channel.channelConsume()
	}
	return Channel.chanName
}


func (Channel *Channel) GetChannel() chan mysql.EventReslut {
	return Channel.chanName
}

func (Channel *Channel) Stop() {
	Channel.Lock()
	defer Channel.Unlock()
	log.Println(Channel.db.Name,"Channel:",Channel.Name,"stop")
	Channel.Status = "stop"
}

func (Channel *Channel) Close() {
	Channel.Lock()
	defer Channel.Unlock()
	log.Println(Channel.db.Name,"Channel:",Channel.Name,"close")
	Channel.Status = "close"
}

func (This *Channel) SetChannelMaxThreadNum(n int) {
	This.Lock()
	defer This.Unlock()
	This.MaxThreadNum = n
}

func (This *Channel) GetChannelMaxThreadNum() int {
	This.Lock()
	defer This.Unlock()
	return This.MaxThreadNum
}

func (c *Channel) channelConsume() {
	c.Lock()
	c.CurrentThreadNum++
	c.Unlock()
	defer func() {
		if err := recover(); err != nil {
			log.Println("channelConsume err:",err)
			c.Lock()
			c.CurrentThreadNum--
			c.Unlock()
		}
	}()
	NewConsumeChannel(c).consume_channel()
}
