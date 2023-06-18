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

package mongo

import (
	"encoding/json"
	"fmt"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"log"
)

func (c *MongoInput) OpLogPosition2GTID(p *primitive.Timestamp) string {
	if p == nil {
		return ""
	}
	return fmt.Sprintf("{\"T\":%d,\"I\":%d}", p.T, p.I)
}

func (c *MongoInput) GTID2OpLogPosition(GTID string) *primitive.Timestamp {
	if GTID == "" {
		return nil
	}
	var p primitive.Timestamp
	err := json.Unmarshal([]byte(GTID), &p)
	if err != nil {
		log.Printf("[ERROR] %s GTID:%s GTID2OpLogPosition err:%+v", c.inputInfo.DbName, GTID, err)
		return nil
	}
	return &p
}
