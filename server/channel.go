
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

	"github.com/jc3wish/Bifrost/Bristol/mysql"
	"github.com/jc3wish/Bifrost/server/count"
	"log"
)

type Channel struct {
	sync.Mutex
	Name string
	chanName         chan mysql.EventReslut
	MaxThreadNum     int // 消费通道的最大线程数
	CurrentThreadNum int
	Status           string //stop ,starting,running,wait
	db               *db
	Errs 			map[int]*ChannelErr
	lastErrId 		int
	countChan		chan  *count.FlowCount
}

type ChannelErr struct {
	WaitErr error
	WaitData interface{}
	WaitDeal int // 0不处理,1错过
}

func NewChannel(MaxThreadNum int,Name string, db *db) *Channel {
	return &Channel{
		Name:Name,
		chanName:             make(chan mysql.EventReslut, MaxThreadNum*100),
		MaxThreadNum:     MaxThreadNum,
		CurrentThreadNum: 0,
		Status:           "stop",
		db:               db,
		Errs: 			  make(map[int]*ChannelErr),
		lastErrId:		  	0,
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

func (Channel *Channel) Start() chan mysql.EventReslut {
	log.Println(Channel.db.Name,"Channel:",Channel.Name,"start")
	Channel.Status = "running"
	for i := 0; i < Channel.MaxThreadNum; i++ {
		go Channel.channelConsume()
	}
	return Channel.chanName
}

func (Channel *Channel) AddWaitError(WaitErr error,WaitData interface{}) int {
	Channel.Lock()
	Channel.lastErrId++
	id := Channel.lastErrId
	Channel.Errs[id] = &ChannelErr{
		WaitErr:WaitErr,
		WaitData:WaitData,
		WaitDeal:0,
	}
	Channel.Unlock()
	return id
}

func (Channel *Channel) DealWaitError(id int) bool {
	Channel.Lock()
	if _,ok:=Channel.Errs[id];!ok{
		Channel.Unlock()
		return false
	}
	Channel.Errs[id].WaitDeal = 1
	Channel.Unlock()
	return true
}

func (Channel *Channel) GetWaitErrorDeal(id int) int {
	Channel.Lock()
	if _,ok:=Channel.Errs[id];!ok{
		Channel.Unlock()
		return -1
	}
	deal := Channel.Errs[id].WaitDeal
	Channel.Unlock()
	return deal
}

func (Channel *Channel) DelWaitError(id int) bool {
	Channel.Lock()
	delete(Channel.Errs,id)
	Channel.Unlock()
	return true
}


func (Channel *Channel) GetChannel() chan mysql.EventReslut {
	return Channel.chanName
}

func (Channel *Channel) Stop() {
	log.Println(Channel.db.Name,"Channel:",Channel.Name,"stop")
	Channel.Status = "stop"
}

func (Channel *Channel) Close() {
	log.Println(Channel.db.Name,"Channel:",Channel.Name,"close")
	Channel.Status = "close"
}

func (This *Channel) SetChannelMaxThreadNum(n int) {
	This.MaxThreadNum = n
}

func (This *Channel) GetChannelMaxThreadNum() int {
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
