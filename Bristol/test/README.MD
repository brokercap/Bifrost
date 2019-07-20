### MySQL_Filed_DataCheck Binlog 解析数据准确性测试


#### 1. 安装docker

docker 安装教程，请参考docker官网

#### 2. 编译  MySQL_Filed_DataCheck

```
go build ./MySQL_Filed_DataCheck.go

```


#### 3. 开始测试

```
chmod a+x MySQL_Filed_DataCheck.sh

./MySQL_Filed_DataCheck.sh 5.6

```

在执行 ./MySQL_Filed_DataCheck.sh 5.6 的时候，脚本会自动下载 mysql:5.6 docker镜像包，然后自动运行 创建一个数据及自动生成数据


#### 4. 连接现有数据库进行测试

```

./MySQL_Filed_DataCheck -u root -p $pwd -h $ip -database test -table ""

```

- u 用户
- p 密码
- h mysql的ip
- database 连接哪一个库
- table 采用哪一个表进行测试，会自动根据表结构自动生成数据，往这个表写入一条数据。假如为空的情况下，会自动创建一个 binlog_field_test 的数据表，表结构为下：

```
			"DROP TABLE IF EXISTS `"+*database+"`.`binlog_field_test`",
			"CREATE TABLE `"+*database+"`.`binlog_field_test` ("+
				"`id` int(11) unsigned NOT NULL AUTO_INCREMENT,"+
				"`testtinyint` tinyint(4) NOT NULL DEFAULT '-1',"+
				"`testsmallint` smallint(6) NOT NULL DEFAULT '-2',"+
				"`testmediumint` mediumint(8) NOT NULL DEFAULT '-3',"+
				"`testint` int(11) NOT NULL DEFAULT '-4',"+
				"`testbigint` bigint(20) NOT NULL DEFAULT '-5',"+
				"`testvarchar` varchar(10) NOT NULL,"+
				"`testchar` char(2) NOT NULL,"+
				"`testenum` enum('en1','en2','en3') NOT NULL DEFAULT 'en1',"+
				"`testset` set('set1','set2','set3') NOT NULL DEFAULT 'set1',"+
				"`testtime` time NOT NULL DEFAULT '00:00:00',"+
				"`testdate` date NOT NULL DEFAULT '0000-00-00',"+
				"`testyear` year(4) NOT NULL DEFAULT '1989',"+
				"`testtimestamp` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,"+
				"`testdatetime` datetime NOT NULL DEFAULT '0000-00-00 00:00:00',"+
				"`testfloat` float(9,2) NOT NULL DEFAULT '0.00',"+
				"`testdouble` double(9,2) NOT NULL DEFAULT '0.00',"+
				"`testdecimal` decimal(9,2) NOT NULL DEFAULT '0.00',"+
				"`testtext` text NOT NULL,"+
				"`testblob` blob NOT NULL,"+
				"`testbit` bit(8) NOT NULL DEFAULT b'0',"+
				"`testbool` tinyint(1) NOT NULL DEFAULT '0',"+
				"`testmediumblob` mediumblob NOT NULL,"+
				"`testlongblob` longblob NOT NULL,"+
				"`testtinyblob` tinyblob NOT NULL,"+
				"`test_unsinged_tinyint` tinyint(4) unsigned NOT NULL DEFAULT '1',"+
				"`test_unsinged_smallint` smallint(6) unsigned NOT NULL DEFAULT '2',"+
				"`test_unsinged_mediumint` mediumint(8) unsigned NOT NULL DEFAULT '3',"+
				"`test_unsinged_int` int(11) unsigned NOT NULL DEFAULT '4',"+
				"`test_unsinged_bigint` bigint(20) unsigned NOT NULL DEFAULT '5',"+
				"PRIMARY KEY (`id`)"+
				") ENGINE=MyISAM AUTO_INCREMENT=0 DEFAULT CHARSET=utf8",

```


#### 5. 判断是否解析全对

```
2019/07/20 14:33:36  type and value is all right

```

在执行结束的时候，假如打印出上面一行日志，则为解析是对的
