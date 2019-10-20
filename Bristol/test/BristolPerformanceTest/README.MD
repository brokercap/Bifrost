# Bristol 性能测试工具

*BristolPerformanceTest* 是用于Bristol的性能测试工具,会自动根据表字段构建数据写入


#### 编译

```
go build ./BristolPerformanceTest.go

```

#### 运行

```
./BristolPerformanceTest -count 1 -u root -p root -P 3306 -conndb test -schema jc3wish_test -table bristol_performance_test

```
```
./BristolPerformanceTest -count 1 -h 127.0.01 -u root -P 3306 -conndb test -schema jc3wish_test -table bristol_performance_test
2019/04/17 22:48:04 ExecSQL success: CREATE DATABASE /*!32312 IF NOT EXISTS*/ `jc3wish_test`
2019/04/17 22:48:04 ExecSQL success: DROP TABLE IF EXISTS jc3wish_test.`bristol_performance_test`
2019/04/17 22:48:04 ExecSQL success: CREATE TABLE jc3wish_test.`bristol_performance_test` (`id` int(11) unsigned NOT NULL AUTO_INCREMENT,`testtinyint` tinyint(4) NOT NULL DEFAULT '-1',`testsmallint` smallint(6) NOT NULL DEFAULT '-2',`testmediumint` mediumint(8) NOT NULL DEFAULT '-3',`testint` int(11) NOT NULL DEFAULT '-4',`testbigint` bigint(20) NOT NULL DEFAULT '-5',`testvarchar` varchar(10) NOT NULL,`testchar` char(2) NOT NULL,`testenum` enum('en1','en2','en3') NOT NULL DEFAULT 'en1',`testset` set('set1','set2','set3') NOT NULL DEFAULT 'set1',`testtime` time NOT NULL DEFAULT '00:00:00',`testdate` date NOT NULL DEFAULT '0000-00-00',`testyear` year(4) NOT NULL DEFAULT '1989',`testtimestamp` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,`testdatetime` datetime NOT NULL DEFAULT '0000-00-00 00:00:00',`testfloat` float(9,2) NOT NULL DEFAULT '0.00',`testdouble` double(9,2) NOT NULL DEFAULT '0.00',`testdecimal` decimal(9,2) NOT NULL DEFAULT '0.00',`testtext` text NOT NULL,`testblob` blob NOT NULL,`testbit` bit(8) NOT NULL DEFAULT b'0',`testbool` tinyint(1) NOT NULL DEFAULT '0',`testmediumblob` mediumblob NOT NULL,`testlongblob` longblob NOT NULL,`testtinyblob` tinyblob NOT NULL,`test_unsinged_tinyint` tinyint(4) unsigned NOT NULL DEFAULT '1',`test_unsinged_smallint` smallint(6) unsigned NOT NULL DEFAULT '2',`test_unsinged_mediumint` mediumint(8) unsigned NOT NULL DEFAULT '3',`test_unsinged_int` int(11) unsigned NOT NULL DEFAULT '4',`test_unsinged_bigint` bigint(20) unsigned NOT NULL DEFAULT '5',PRIMARY KEY (`id`)) ENGINE=MyISAM AUTO_INCREMENT=0 DEFAULT CHARSET=utf8
2019/04/17 22:48:04 insert data start
2019/04/17 22:48:04 insert data over
2019/04/17 22:48:04 use time 0 s
2019/04/17 22:48:04 analysis binlog start
2019/04/17 22:48:04 start binlog info: mysql-bin.000015 88324035
2019/04/17 22:48:04 root:@tcp(127.0.01:3306)/test start DumpBinlog...
2019/04/17 22:48:04 monitor reslut:starting
2019/04/17 22:48:04 analysis binlog over
2019/04/17 22:48:04 analysis success count: 1
2019/04/17 22:48:04 end binlog info: mysql-bin.000015 88324476
2019/04/17 22:48:04 use time: 0 s

```


#### 参数解析

> -h 127.0.0.1 : mysql 连接ip, 默认 127.0.0.1
>
> -u root      : mysql 用户名, 默认 root
>
> -p root      : mysql 密码, 默认为空
>
> -P 3306      : mysql 端口号, 默认 3306
>
> -conndb test : 连接mysql时候的数据库名,默认 test
>
> -schema      : 性能测试数据所有库,如果不存在则自动创建 ,默认 jc3wish_test
>
> -table       : 性能测试数据所在表,如果表不存在则会自动删除再创建 , 默认 bristol_performance_test

> -count       : 测试多少条数据,默认 100000

> -onlydata    : true的话,只造数据,不做二进制解析,默认 false,使用方法为  -onydata true

> -master_log_file    : mysql 当前的位点文件，

> -master_log_pos     : mysql 当前的位点文件位点，当master_log_file及 master_log_pos 存在的时候，不会创建数据，直接去解析binlog