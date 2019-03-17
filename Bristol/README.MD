## Bristol ---- 模拟MySQL从库连接,监听MySQL binlog变更并解析

#### DDL 支持说明

当前只支持字段在表结构末尾追加新字段，如果配置的二进制位点是在DDL 之前的位点，会出现数据和字段对应不上
数据类型修改之后，如果配置的二进制位点是在DDL 之前的位点，可能会出现数据出错

#### 类型转换

从Binlog解析出来的数据,在转成 map[string]interface{} 的数据格式的时候
MySQL里的存储类型对应Golang里的数据类型不一样
括号里的代表是Golang里的数据类型


- [x] - TINYINT ( int8 | unit8 )
- [x] - SMALLINT ( int16 | unit16 )
- [x] - MEDIUMINT ( int32 | uint32 )
- [x] - INT ( int32 | uint32 )
- [x] - BIGINT ( int64 | uint64 )
- [x] - FLOAT ( float32 )
- [x] - DOUBLE ( float64 )
- [x] - REAL ( float64 )
- [x] - DECIMAL , NUMERIC( 返回string类型 )
- [x] - DATE , TIME , YEAR , DATETIME , TIMESTAMP   ( string类型 )
- [x] - CHAR , VARCHAR ( string )
- [x] - TEXT , TINYTEXT , MEDIUMINTTEXT , - LONGTEXT ( string )
- [x] - BLOB, TINYBLOB , MEDIUMINTBLOB , LONGBLOB ( string )
- [x] - ENUM ( string类型 )
- [x] - SET ( []string )
- [x] BIT ( int64类型 )

#### 使用案例
`````go
import (
	"log"

	"github.com/Bristol/mysql"
	"time"
)

func callback(data *mysql.EventReslut) {
	log.Println(data)
}

func main() {
	filename := "mysql-bin.000022"
	var position uint32 = 13333
	reslut := make(chan error, 1)
	m := make(map[string]uint8, 0)
	m["testdbcreate"] = 1
	BinlogDump := &mysql.BinlogDump{
		DataSource:    "root:root@tcp(127.0.0.1:3306)/test",
		CallbackFun:   callback,
		ReplicateDoDb: m,
		OnlyEvent:     []mysql.EventType{mysql.QUERY_EVENT, mysql.WRITE_ROWS_EVENTv1, mysql.UPDATE_ROWS_EVENTv1, mysql.DELETE_ROWS_EVENTv1},
	}
	go BinlogDump.StartDumpBinlog(filename, position, 100,reslut,"",0)
	go func() {
		v := <-reslut
		log.Printf("monitor reslut:%s \r\n", v)
	}()
	for {
		time.Sleep(10 * time.Second)
	}
}

`````

#### 数据检验

```
cd Bristol/test
go build MySQL_Filed_DataCheck.go

./MySQL_Filed_DataCheck -h 127.0.0.1 -u root -p password -database test

2019/03/17 14:09:03 load data start
2019/03/17 14:09:03 exec sql: CREATE DATABASE /*!32312 IF NOT EXISTS*/ `bifrost_test`
2019/03/17 14:09:03 exec sql: DROP TABLE IF EXISTS bifrost_test.`binlog_field_test`
2019/03/17 14:09:03 exec sql: CREATE TABLE bifrost_test.`binlog_field_test` (`id` int(11) unsigned NOT NULL AUTO_INCREMENT,`testtinyint` tinyint(4) NOT NULL DEFAULT '-1',`testsmallint` smallint(6) NOT NULL DEFAULT '-2',`testmediumint` mediumint(8) NOT NULL DEFAULT '-3',`testint` int(11) NOT NULL DEFAULT '-4',`testbigint` bigint(20) NOT NULL DEFAULT '-5',`testvarchar` varchar(10) NOT NULL,`testchar` char(2) NOT NULL,`testenum` enum('en1','en2','en3') NOT NULL DEFAULT 'en1',`testset` set('set1','set2','set3') NOT NULL DEFAULT 'set1',`testtime` time NOT NULL DEFAULT '00:00:00',`testdate` date NOT NULL DEFAULT '0000-00-00',`testyear` year(4) NOT NULL DEFAULT '1989',`testtimestamp` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,`testdatetime` datetime NOT NULL DEFAULT '0000-00-00 00:00:00',`testfloat` float(9,2) NOT NULL DEFAULT '0.00',`testdouble` double(9,2) NOT NULL DEFAULT '0.00',`testdecimal` decimal(9,2) NOT NULL DEFAULT '0.00',`testtext` text NOT NULL,`testblob` blob NOT NULL,`testbit` bit(8) NOT NULL DEFAULT b'0',`testbool` tinyint(1) NOT NULL DEFAULT '0',`testmediumblob` mediumblob NOT NULL,`testlongblob` longblob NOT NULL,`testtinyblob` tinyblob NOT NULL,`test_unsinged_tinyint` tinyint(4) unsigned NOT NULL DEFAULT '1',`test_unsinged_smallint` smallint(6) unsigned NOT NULL DEFAULT '2',`test_unsinged_mediumint` mediumint(8) unsigned NOT NULL DEFAULT '3',`test_unsinged_int` int(11) unsigned NOT NULL DEFAULT '4',`test_unsinged_bigint` bigint(20) unsigned NOT NULL DEFAULT '5',PRIMARY KEY (`id`)) ENGINE=MyISAM AUTO_INCREMENT=2 DEFAULT CHARSET=utf8
2019/03/17 14:09:03 exec sql: insert  into bifrost_test.`binlog_field_test`(`id`,`testtinyint`,`testsmallint`,`testmediumint`,`testint`,`testbigint`,`testvarchar`,`testchar`,`testenum`,`testset`,`testtime`,`testdate`,`testyear`,`testtimestamp`,`testdatetime`,`testfloat`,`testdouble`,`testdecimal`,`testtext`,`testblob`,`testbit`,`testbool`,`testmediumblob`,`testlongblob`,`testtinyblob`,`test_unsinged_tinyint`,`test_unsinged_smallint`,`test_unsinged_mediumint`,`test_unsinged_int`,`test_unsinged_bigint`) values (1,-1,-2,-3,-4,-5,'testvarcha','te','en2','set1,set3','15:39:59','2018-05-08',2018,'2018-05-08 15:30:21','2018-05-08 15:30:21',9.39,9.39,9.39,'testtext','testblob',',1,'testmediumblob','testlongblob','testtinyblob',1,2,3,4,5)
2019/03/17 14:09:03 load data over
2019/03/17 14:09:03 root:@tcp(127.0.0.1:3306)/test start DumpBinlog...
2019/03/17 14:09:03 monitor reslut:starting
2019/03/17 14:09:03 monitor reslut:running
2019/03/17 14:09:03 CREATE DATABASE /*!32312 IF NOT EXISTS*/ `bifrost_test`
schema: bifrost_test table:  EventType: 2
schema: bifrost_test table: binlog_field_test EventType: insert
2019/03/17 14:09:03 testchar te == te filed-Type: char golang-type: string  is right
2019/03/17 14:09:03 testdate 2018-05-08 == 2018-05-08 filed-Type: date golang-type: string  is right
2019/03/17 14:09:03 testtimestamp 2018-05-08 15:30:21 == 2018-05-08 15:30:21 filed-Type: timestamp golang-type: string  is right
2019/03/17 14:09:03 testtext testtext == testtext filed-Type: text golang-type: string  is right
2019/03/17 14:09:03 test_unsinged_int 4 == 4 filed-Type: unsinged_int golang-type: uint32  is right
2019/03/17 14:09:03 testvarchar testvarcha == testvarcha filed-Type: varchar golang-type: string  is right
2019/03/17 14:09:03 testtime 15:39:59 == 15:39:59 filed-Type: time golang-type: string  is right
2019/03/17 14:09:03 testmediumint -3 == -3 filed-Type: mediumint golang-type: int32  is right
2019/03/17 14:09:03 testdouble 9.39 == 9.39 filed-Type: double golang-type: float64  is right
2019/03/17 14:09:03 testbool true == true filed-Type: bool golang-type: bool  is right
2019/03/17 14:09:03 test_unsinged_tinyint 1 == 1 filed-Type: unsinged_tinyint golang-type: uint8  is right
2019/03/17 14:09:03 test_unsinged_smallint 2 == 2 filed-Type: unsinged_smallint golang-type: uint16  is right
2019/03/17 14:09:03 testsmallint -2 == -2 filed-Type: smallint golang-type: int16  is right
2019/03/17 14:09:03 testbigint -5 == -5 filed-Type: bigint golang-type: int64  is right
2019/03/17 14:09:03 testfloat 9.39 == 9.39 filed-Type: float golang-type: float32  is right
2019/03/17 14:09:03 testset (set1,set3) == [set1 set3] filed-Type: set golang-type: []string  is right
2019/03/17 14:09:03 testyear 2018 == 2018 filed-Type: year golang-type: string  is right
2019/03/17 14:09:03 testdecimal 9.39 == 9.39 filed-Type: decimal golang-type: string  is right
2019/03/17 14:09:03 testblob testblob == testblob filed-Type: blob golang-type: string  is right
2019/03/17 14:09:03 testtinyblob testtinyblob == testtinyblob filed-Type: tinyblob golang-type: string  is right
2019/03/17 14:09:03 testenum en2 == en2 filed-Type: enum golang-type: string  is right
2019/03/17 14:09:03 testbit 8 == 8 filed-Type: bit golang-type: int64  is right
2019/03/17 14:09:03 testlongblob testlongblob == testlongblob filed-Type: longblob golang-type: string  is right
2019/03/17 14:09:03 testtinyint 1 == -1 filed-Type: tinyint golang-type: int8  is right
2019/03/17 14:09:03 testint -4 == -4 filed-Type: int golang-type: int32  is right
2019/03/17 14:09:03 test_unsinged_bigint 5 == 5 filed-Type: unsinged_bigint golang-type: uint64  is right
2019/03/17 14:09:03 id 1 == 1 filed-Type: uint golang-type: uint32  is right
2019/03/17 14:09:03 testdatetime 2018-05-08 15:30:21 == 2018-05-08 15:30:21 filed-Type: datetime golang-type: string  is right
2019/03/17 14:09:03 testmediumblob testmediumblob == testmediumblob filed-Type: mediumblob golang-type: string  is right
2019/03/17 14:09:03 test_unsinged_mediumint 3 == 3 filed-Type: unsinged_mediumint golang-type: uint32  is right
2019/03/17 14:09:03  type and value is all right
```

- database 参数是只用于连接MySQL 使用

- 运行 MySQL_Filed_DataCheck 的时候,会自动创建 一个 *bifrost_test* 数据库,及创建一个数据表 *binlog_field_test* ,并且往 *binlog_field_test* 数据表中写入数据,然后解析解析二进制数据,及与insert数据的数据对比
- 最后返回 type and value is all right 则为全部解析正常


##### sql
```
/*
SQLyog Ultimate v10.42
MySQL - 5.5.42-log : Database - bifrost_test
*********************************************************************
*/


/*!40101 SET NAMES utf8 */;

/*!40101 SET SQL_MODE=''*/;

/*!40014 SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0 */;
/*!40014 SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0 */;
/*!40101 SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='NO_AUTO_VALUE_ON_ZERO' */;
/*!40111 SET @OLD_SQL_NOTES=@@SQL_NOTES, SQL_NOTES=0 */;
CREATE DATABASE /*!32312 IF NOT EXISTS*/`bifrost_test` /*!40100 DEFAULT CHARACTER SET utf8 */;

USE `bifrost_test`;

/*Table structure for table `test3` */

DROP TABLE IF EXISTS `binlog_field_test`;

CREATE TABLE `binlog_field_test` (
  `id` int(11) unsigned NOT NULL AUTO_INCREMENT,
  `testtinyint` tinyint(4) NOT NULL DEFAULT '-1',
  `testsmallint` smallint(6) NOT NULL DEFAULT '-2',
  `testmediumint` mediumint(8) NOT NULL DEFAULT '-3',
  `testint` int(11) NOT NULL DEFAULT '-4',
  `testbigint` bigint(20) NOT NULL DEFAULT '-5',
  `testvarchar` varchar(10) NOT NULL,
  `testchar` char(2) NOT NULL,
  `testenum` enum('en1','en2','en3') NOT NULL DEFAULT 'en1',
  `testset` set('set1','set2','set3') NOT NULL DEFAULT 'set1',
  `testtime` time NOT NULL DEFAULT '00:00:00',
  `testdate` date NOT NULL DEFAULT '0000-00-00',
  `testyear` year(4) NOT NULL DEFAULT '1989',
  `testtimestamp` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `testdatetime` datetime NOT NULL DEFAULT '0000-00-00 00:00:00',
  `testfloat` float(9,2) NOT NULL DEFAULT '0.00',
  `testdouble` double(9,2) NOT NULL DEFAULT '0.00',
  `testdecimal` decimal(9,2) NOT NULL DEFAULT '0.00',
  `testtext` text NOT NULL,
  `testblob` blob NOT NULL,
  `testbit` bit(8) NOT NULL DEFAULT b'0',
  `testbool` tinyint(1) NOT NULL DEFAULT '0',
  `testmediumblob` mediumblob NOT NULL,
  `testlongblob` longblob NOT NULL,
  `testtinyblob` tinyblob NOT NULL,
  `test_unsinged_tinyint` tinyint(4) unsigned NOT NULL DEFAULT '1',
  `test_unsinged_smallint` smallint(6) unsigned NOT NULL DEFAULT '2',
  `test_unsinged_mediumint` mediumint(8) unsigned NOT NULL DEFAULT '3',
  `test_unsinged_int` int(11) unsigned NOT NULL DEFAULT '4',
  `test_unsinged_bigint` bigint(20) unsigned NOT NULL DEFAULT '5',
  PRIMARY KEY (`id`)
) ENGINE=MyISAM AUTO_INCREMENT=2 DEFAULT CHARSET=utf8;

/*Data for the table `test3` */

insert  into `binlog_field_test`(`id`,`testtinyint`,`testsmallint`,`testmediumint`,`testint`,`testbigint`,`testvarchar`,`testchar`,`testenum`,`testset`,`testtime`,`testdate`,`testyear`,`testtimestamp`,`testdatetime`,`testfloat`,`testdouble`,`testdecimal`,`testtext`,`testblob`,`testbit`,`testbool`,`testmediumblob`,`testlongblob`,`testtinyblob`,`test_unsinged_tinyint`,`test_unsinged_smallint`,`test_unsinged_mediumint`,`test_unsinged_int`,`test_unsinged_bigint`) values (1,-1,-2,-3,-4,-5,'testvarcha','te','en2','set1,set3','15:39:59','2018-05-08',2018,'2018-05-08 15:30:21','2018-05-08 15:30:21',9.39,9.39,9.39,'testtext','testblob','',1,'testmediumblob','testlongblob','testtinyblob',1,2,3,4,5);

/*!40101 SET SQL_MODE=@OLD_SQL_MODE */;
/*!40014 SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS */;
/*!40014 SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS */;
/*!40111 SET SQL_NOTES=@OLD_SQL_NOTES */;

```