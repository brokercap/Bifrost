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
	"context"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"reflect"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/bsontype"
)

func CreateMongoClient(uri string, ctx context.Context) (*mongo.Client, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	rb := bson.NewRegistryBuilder()
	rb.RegisterTypeMapEntry(bsontype.DateTime, reflect.TypeOf(time.Time{}))
	reg := rb.Build()
	clientOptions := options.Client()
	clientOptions.SetRegistry(reg)
	clientOptions.ApplyURI(uri)
	timeOutCtx, _ := context.WithTimeout(ctx, 15*time.Second)
	client, err := mongo.Connect(timeOutCtx, clientOptions)
	if err != nil {
		return nil, err
	}
	return client, nil
}
