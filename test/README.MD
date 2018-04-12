#### 创建测试数据库

mysqldump -uroot -proot < ./Bifrost_test.db.sql

### 新建新数据

`````go
go run insertDataTest.go -host 127.0.0.1 -user root -pwd root -port 3306 -schema bifrost_test -table test1 -count 100000

`````