package pluginTestData

import (
	"bytes"
	"encoding/gob"
	"encoding/json"
	"fmt"
	pluginDriver "github.com/brokercap/Bifrost/plugin/driver"
	"math/rand"
	"strconv"
	"strings"
	"time"
)

var MysqlCreateTalbeSQL = "CREATE TABLE `bifrost_test`.`binlog_field_test` (`id` int(11) unsigned NOT NULL AUTO_INCREMENT,`testtinyint` tinyint(4) NOT NULL DEFAULT '-1',`testsmallint` smallint(6) NOT NULL DEFAULT '-2',`testmediumint` mediumint(8) NOT NULL DEFAULT '-3',`testint` int(11) NOT NULL DEFAULT '-4',`testbigint` bigint(20) NOT NULL DEFAULT '-5',`testvarchar` varchar(10) NOT NULL,`testchar` char(2) NOT NULL,`testenum` enum('en1','en2','en3') NOT NULL DEFAULT 'en1',`testset` set('set1','set2','set3') NOT NULL DEFAULT 'set1',`testtime` time NOT NULL DEFAULT '00:00:00',`testdate` date NOT NULL DEFAULT '0000-00-00',`testyear` year(4) NOT NULL DEFAULT '1989',`testtimestamp` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,`testdatetime` datetime NOT NULL DEFAULT '0000-00-00 00:00:00',`testfloat` float(9,2) NOT NULL DEFAULT '0.00',`testdouble` double(9,2) NOT NULL DEFAULT '0.00',`testdecimal` decimal(9,2) NOT NULL DEFAULT '0.00',`testdecimal2` decimal(10,4) NOT NULL DEFAULT '0.00',`testdecimal3` decimal(20,4) NOT NULL DEFAULT '0.00',`testdecimal4` decimal(30,5) NOT NULL DEFAULT '0.00',`testtext` text NOT NULL,`testblob` blob NOT NULL,`testbit` bit(8) NOT NULL DEFAULT b'0',`testbool` tinyint(1) NOT NULL DEFAULT '0',`testmediumblob` mediumblob NOT NULL,`testlongblob` longblob NOT NULL,`testtinyblob` tinyblob NOT NULL,`test_unsinged_tinyint` tinyint(4) unsigned NOT NULL DEFAULT '1',`test_unsinged_smallint` smallint(6) unsigned NOT NULL DEFAULT '2',`test_unsinged_mediumint` mediumint(8) unsigned NOT NULL DEFAULT '3',`test_unsinged_int` int(11) unsigned NOT NULL DEFAULT '4',`test_unsinged_bigint` bigint(20) unsigned NOT NULL DEFAULT '5',`testtime2_1` time(1) NOT NULL DEFAULT '00:00:00.0',`testtime2_2` time(2) NOT NULL DEFAULT '00:00:00.00',`testtime2_3` time(3) NOT NULL DEFAULT '00:00:00.000',`testtime2_4` time(4) NOT NULL DEFAULT '00:00:00.0000',`testtime2_5` time(5) NOT NULL DEFAULT '00:00:00.00000',`testtime2_6` time(6) NOT NULL DEFAULT '00:00:00.000000',`testtimestamp2_1` timestamp(1) NOT NULL DEFAULT CURRENT_TIMESTAMP(1),`testtimestamp2_2` timestamp(2) NOT NULL DEFAULT CURRENT_TIMESTAMP(2),`testtimestamp2_3` timestamp(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),`testtimestamp2_4` timestamp(4) NOT NULL DEFAULT CURRENT_TIMESTAMP(4),`testtimestamp2_5` timestamp(5) NOT NULL DEFAULT CURRENT_TIMESTAMP(5),`testtimestamp2_6` timestamp(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),`testdatetime2_1` datetime(1) NOT NULL DEFAULT '0000-00-00 00:00:00.0',`testdatetime2_2` datetime(2) NOT NULL DEFAULT '0000-00-00 00:00:00.00',`testdatetime2_3` datetime(3) NOT NULL DEFAULT '0000-00-00 00:00:00.000',`testdatetime2_4` datetime(4) NOT NULL DEFAULT '0000-00-00 00:00:00.0000',`testdatetime2_5` datetime(5) NOT NULL DEFAULT '0000-00-00 00:00:00.00000',`testdatetime2_6` datetime(6) NOT NULL DEFAULT '0000-00-00 00:00:00.000000',`test_json` json,PRIMARY KEY (`id`)) ENGINE=MyISAM AUTO_INCREMENT=0 DEFAULT CHARSET=utf8"

/*
CREATE TABLE `bifrost_test`.`binlog_field_test` (
  `id` INT (11) UNSIGNED NOT NULL AUTO_INCREMENT,
  `testtinyint` TINYINT (4) NOT NULL DEFAULT '-1',
  `testsmallint` SMALLINT (6) NOT NULL DEFAULT '-2',
  `testmediumint` MEDIUMINT (8) NOT NULL DEFAULT '-3',
  `testint` INT (11) NOT NULL DEFAULT '-4',
  `testbigint` BIGINT (20) NOT NULL DEFAULT '-5',
  `testvarchar` VARCHAR (10) NOT NULL,
  `testchar` CHAR(2) NOT NULL,
  `testenum` ENUM ('en1', 'en2', 'en3') NOT NULL DEFAULT 'en1',
  `testset` SET ('set1', 'set2', 'set3') NOT NULL DEFAULT 'set1',
  `testtime` TIME NOT NULL DEFAULT '00:00:00',
  `testdate` DATE NOT NULL DEFAULT '0000-00-00',
  `testyear` YEAR(4) NOT NULL DEFAULT '1989',
  `testtimestamp` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `testdatetime` DATETIME NOT NULL DEFAULT '0000-00-00 00:00:00',
  `testfloat` FLOAT (9, 2) NOT NULL DEFAULT '0.00',
  `testdouble` DOUBLE (9, 2) NOT NULL DEFAULT '0.00',
  `testdecimal` DECIMAL (9, 2) NOT NULL DEFAULT '0.00',
  `testdecimal2` DECIMAL (10, 4) NOT NULL DEFAULT '0.00',
  `testdecimal3` DECIMAL (20, 4) NOT NULL DEFAULT '0.00',
  `testdecimal4` DECIMAL (30, 5) NOT NULL DEFAULT '0.00',
  `testtext` TEXT NOT NULL,
  `testblob` BLOB NOT NULL,
  `testbit` BIT (8) NOT NULL DEFAULT b'0',
  `testbool` TINYINT (1) NOT NULL DEFAULT '0',
  `testmediumblob` MEDIUMBLOB NOT NULL,
  `testlongblob` LONGBLOB NOT NULL,
  `testtinyblob` TINYBLOB NOT NULL,
  `test_unsinged_tinyint` TINYINT (4) UNSIGNED NOT NULL DEFAULT '1',
  `test_unsinged_smallint` SMALLINT (6) UNSIGNED NOT NULL DEFAULT '2',
  `test_unsinged_mediumint` MEDIUMINT (8) UNSIGNED NOT NULL DEFAULT '3',
  `test_unsinged_int` INT (11) UNSIGNED NOT NULL DEFAULT '4',
  `test_unsinged_bigint` BIGINT (20) UNSIGNED NOT NULL DEFAULT '5',
  `testtime2_1` TIME(1) NOT NULL DEFAULT '00:00:00.0',
  `testtime2_2` TIME(2) NOT NULL DEFAULT '00:00:00.00',
  `testtime2_3` TIME(3) NOT NULL DEFAULT '00:00:00.000',
  `testtime2_4` TIME(4) NOT NULL DEFAULT '00:00:00.0000',
  `testtime2_5` TIME(5) NOT NULL DEFAULT '00:00:00.00000',
  `testtime2_6` TIME(6) NOT NULL DEFAULT '00:00:00.000000',
  `testtimestamp2_1` TIMESTAMP(1) NOT NULL DEFAULT CURRENT_TIMESTAMP(1),
  `testtimestamp2_2` TIMESTAMP(2) NOT NULL DEFAULT CURRENT_TIMESTAMP(2),
  `testtimestamp2_3` TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  `testtimestamp2_4` TIMESTAMP(4) NOT NULL DEFAULT CURRENT_TIMESTAMP(4),
  `testtimestamp2_5` TIMESTAMP(5) NOT NULL DEFAULT CURRENT_TIMESTAMP(5),
  `testtimestamp2_6` TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
  `testdatetime2_1` DATETIME (1) NOT NULL DEFAULT '0000-00-00 00:00:00.0',
  `testdatetime2_2` DATETIME (2) NOT NULL DEFAULT '0000-00-00 00:00:00.00',
  `testdatetime2_3` DATETIME (3) NOT NULL DEFAULT '0000-00-00 00:00:00.000',
  `testdatetime2_4` DATETIME (4) NOT NULL DEFAULT '0000-00-00 00:00:00.0000',
  `testdatetime2_5` DATETIME (5) NOT NULL DEFAULT '0000-00-00 00:00:00.00000',
  `testdatetime2_6` DATETIME (6) NOT NULL DEFAULT '0000-00-00 00:00:00.000000',
  `test_json` json,
  PRIMARY KEY (`id`)
) ENGINE = MYISAM AUTO_INCREMENT = 0 DEFAULT CHARSET = utf8
*/

var columnJsonString = `[{"ColumnName":"id","ColumnKey":"PRI","ColumnDefault":"NULL","DataType":"int","Extra":"auto_increment","ColumnType":"int unsigned","CharacterSetName":"NULL","CollationName":"NULL","NumbericScale":0,"IsBool":false,"Unsigned":true,"IsPrimary":true,"AutoIncrement":true,"EnumValues":[],"SetValues":[],"CharacterMaximumLength":0,"NumbericPrecision":10,"Fsp":0,"IsNullable":"NO","Value":null},{"ColumnName":"testtinyint","ColumnKey":"","ColumnDefault":"-1","DataType":"tinyint","Extra":"","ColumnType":"tinyint","CharacterSetName":"NULL","CollationName":"NULL","NumbericScale":0,"IsBool":false,"Unsigned":false,"IsPrimary":false,"AutoIncrement":false,"EnumValues":[],"SetValues":[],"CharacterMaximumLength":0,"NumbericPrecision":3,"Fsp":0,"IsNullable":"NO","Value":null},{"ColumnName":"testsmallint","ColumnKey":"","ColumnDefault":"-2","DataType":"smallint","Extra":"","ColumnType":"smallint","CharacterSetName":"NULL","CollationName":"NULL","NumbericScale":0,"IsBool":false,"Unsigned":false,"IsPrimary":false,"AutoIncrement":false,"EnumValues":[],"SetValues":[],"CharacterMaximumLength":0,"NumbericPrecision":5,"Fsp":0,"IsNullable":"NO","Value":null},{"ColumnName":"testmediumint","ColumnKey":"","ColumnDefault":"-3","DataType":"mediumint","Extra":"","ColumnType":"mediumint","CharacterSetName":"NULL","CollationName":"NULL","NumbericScale":0,"IsBool":false,"Unsigned":false,"IsPrimary":false,"AutoIncrement":false,"EnumValues":[],"SetValues":[],"CharacterMaximumLength":0,"NumbericPrecision":7,"Fsp":0,"IsNullable":"NO","Value":null},{"ColumnName":"testint","ColumnKey":"","ColumnDefault":"-4","DataType":"int","Extra":"","ColumnType":"int","CharacterSetName":"NULL","CollationName":"NULL","NumbericScale":0,"IsBool":false,"Unsigned":false,"IsPrimary":false,"AutoIncrement":false,"EnumValues":[],"SetValues":[],"CharacterMaximumLength":0,"NumbericPrecision":10,"Fsp":0,"IsNullable":"NO","Value":null},{"ColumnName":"testbigint","ColumnKey":"","ColumnDefault":"-5","DataType":"bigint","Extra":"","ColumnType":"bigint","CharacterSetName":"NULL","CollationName":"NULL","NumbericScale":0,"IsBool":false,"Unsigned":false,"IsPrimary":false,"AutoIncrement":false,"EnumValues":[],"SetValues":[],"CharacterMaximumLength":0,"NumbericPrecision":19,"Fsp":0,"IsNullable":"NO","Value":null},{"ColumnName":"testvarchar","ColumnKey":"","ColumnDefault":"NULL","DataType":"varchar","Extra":"","ColumnType":"varchar(10)","CharacterSetName":"utf8","CollationName":"utf8_general_ci","NumbericScale":0,"IsBool":false,"Unsigned":false,"IsPrimary":false,"AutoIncrement":false,"EnumValues":[],"SetValues":[],"CharacterMaximumLength":10,"NumbericPrecision":0,"Fsp":0,"IsNullable":"NO","Value":null},{"ColumnName":"testchar","ColumnKey":"","ColumnDefault":"NULL","DataType":"char","Extra":"","ColumnType":"char(2)","CharacterSetName":"utf8","CollationName":"utf8_general_ci","NumbericScale":0,"IsBool":false,"Unsigned":false,"IsPrimary":false,"AutoIncrement":false,"EnumValues":[],"SetValues":[],"CharacterMaximumLength":2,"NumbericPrecision":0,"Fsp":0,"IsNullable":"NO","Value":null},{"ColumnName":"testenum","ColumnKey":"","ColumnDefault":"en1","DataType":"enum","Extra":"","ColumnType":"enum('en1','en2','en3')","CharacterSetName":"utf8","CollationName":"utf8_general_ci","NumbericScale":0,"IsBool":false,"Unsigned":false,"IsPrimary":false,"AutoIncrement":false,"EnumValues":["en1","en2","en3"],"SetValues":[],"CharacterMaximumLength":3,"NumbericPrecision":0,"Fsp":0,"IsNullable":"NO","Value":null},{"ColumnName":"testset","ColumnKey":"","ColumnDefault":"set1","DataType":"set","Extra":"","ColumnType":"set('set1','set2','set3')","CharacterSetName":"utf8","CollationName":"utf8_general_ci","NumbericScale":0,"IsBool":false,"Unsigned":false,"IsPrimary":false,"AutoIncrement":false,"EnumValues":[],"SetValues":["set1","set2","set3"],"CharacterMaximumLength":14,"NumbericPrecision":0,"Fsp":0,"IsNullable":"NO","Value":null},{"ColumnName":"testtime","ColumnKey":"","ColumnDefault":"00:00:00","DataType":"time","Extra":"","ColumnType":"time","CharacterSetName":"NULL","CollationName":"NULL","NumbericScale":0,"IsBool":false,"Unsigned":false,"IsPrimary":false,"AutoIncrement":false,"EnumValues":[],"SetValues":[],"CharacterMaximumLength":0,"NumbericPrecision":0,"Fsp":0,"IsNullable":"NO","Value":null},{"ColumnName":"testdate","ColumnKey":"","ColumnDefault":"0000-00-00","DataType":"date","Extra":"","ColumnType":"date","CharacterSetName":"NULL","CollationName":"NULL","NumbericScale":0,"IsBool":false,"Unsigned":false,"IsPrimary":false,"AutoIncrement":false,"EnumValues":[],"SetValues":[],"CharacterMaximumLength":0,"NumbericPrecision":0,"Fsp":0,"IsNullable":"NO","Value":null},{"ColumnName":"testyear","ColumnKey":"","ColumnDefault":"1989","DataType":"year","Extra":"","ColumnType":"year","CharacterSetName":"NULL","CollationName":"NULL","NumbericScale":0,"IsBool":false,"Unsigned":false,"IsPrimary":false,"AutoIncrement":false,"EnumValues":[],"SetValues":[],"CharacterMaximumLength":0,"NumbericPrecision":0,"Fsp":0,"IsNullable":"NO","Value":null},{"ColumnName":"testtimestamp","ColumnKey":"","ColumnDefault":"CURRENT_TIMESTAMP","DataType":"timestamp","Extra":"DEFAULT_GENERATED","ColumnType":"timestamp","CharacterSetName":"NULL","CollationName":"NULL","NumbericScale":0,"IsBool":false,"Unsigned":false,"IsPrimary":false,"AutoIncrement":false,"EnumValues":[],"SetValues":[],"CharacterMaximumLength":0,"NumbericPrecision":0,"Fsp":0,"IsNullable":"NO","Value":null},{"ColumnName":"testdatetime","ColumnKey":"","ColumnDefault":"0000-00-00 00:00:00","DataType":"datetime","Extra":"","ColumnType":"datetime","CharacterSetName":"NULL","CollationName":"NULL","NumbericScale":0,"IsBool":false,"Unsigned":false,"IsPrimary":false,"AutoIncrement":false,"EnumValues":[],"SetValues":[],"CharacterMaximumLength":0,"NumbericPrecision":0,"Fsp":0,"IsNullable":"NO","Value":null},{"ColumnName":"testfloat","ColumnKey":"","ColumnDefault":"0.00","DataType":"float","Extra":"","ColumnType":"float(9,2)","CharacterSetName":"NULL","CollationName":"NULL","NumbericScale":2,"IsBool":false,"Unsigned":false,"IsPrimary":false,"AutoIncrement":false,"EnumValues":[],"SetValues":[],"CharacterMaximumLength":0,"NumbericPrecision":9,"Fsp":0,"IsNullable":"NO","Value":null},{"ColumnName":"testdouble","ColumnKey":"","ColumnDefault":"0.00","DataType":"double","Extra":"","ColumnType":"double(9,2)","CharacterSetName":"NULL","CollationName":"NULL","NumbericScale":2,"IsBool":false,"Unsigned":false,"IsPrimary":false,"AutoIncrement":false,"EnumValues":[],"SetValues":[],"CharacterMaximumLength":0,"NumbericPrecision":9,"Fsp":0,"IsNullable":"NO","Value":null},{"ColumnName":"testdecimal","ColumnKey":"","ColumnDefault":"0.00","DataType":"decimal","Extra":"","ColumnType":"decimal(9,2)","CharacterSetName":"NULL","CollationName":"NULL","NumbericScale":2,"IsBool":false,"Unsigned":false,"IsPrimary":false,"AutoIncrement":false,"EnumValues":[],"SetValues":[],"CharacterMaximumLength":0,"NumbericPrecision":9,"Fsp":0,"IsNullable":"NO","Value":null},{"ColumnName":"testdecimal2","ColumnKey":"","ColumnDefault":"0.0000","DataType":"decimal","Extra":"","ColumnType":"decimal(10,4)","CharacterSetName":"NULL","CollationName":"NULL","NumbericScale":4,"IsBool":false,"Unsigned":false,"IsPrimary":false,"AutoIncrement":false,"EnumValues":[],"SetValues":[],"CharacterMaximumLength":0,"NumbericPrecision":10,"Fsp":0,"IsNullable":"NO","Value":null},{"ColumnName":"testdecimal3","ColumnKey":"","ColumnDefault":"0.0000","DataType":"decimal","Extra":"","ColumnType":"decimal(20,4)","CharacterSetName":"NULL","CollationName":"NULL","NumbericScale":4,"IsBool":false,"Unsigned":false,"IsPrimary":false,"AutoIncrement":false,"EnumValues":[],"SetValues":[],"CharacterMaximumLength":0,"NumbericPrecision":20,"Fsp":0,"IsNullable":"NO","Value":null},{"ColumnName":"testdecimal4","ColumnKey":"","ColumnDefault":"0.00000","DataType":"decimal","Extra":"","ColumnType":"decimal(30,5)","CharacterSetName":"NULL","CollationName":"NULL","NumbericScale":5,"IsBool":false,"Unsigned":false,"IsPrimary":false,"AutoIncrement":false,"EnumValues":[],"SetValues":[],"CharacterMaximumLength":0,"NumbericPrecision":30,"Fsp":0,"IsNullable":"NO","Value":null},{"ColumnName":"testtext","ColumnKey":"","ColumnDefault":"NULL","DataType":"text","Extra":"","ColumnType":"text","CharacterSetName":"utf8","CollationName":"utf8_general_ci","NumbericScale":0,"IsBool":false,"Unsigned":false,"IsPrimary":false,"AutoIncrement":false,"EnumValues":[],"SetValues":[],"CharacterMaximumLength":65535,"NumbericPrecision":0,"Fsp":0,"IsNullable":"NO","Value":null},{"ColumnName":"testblob","ColumnKey":"","ColumnDefault":"NULL","DataType":"blob","Extra":"","ColumnType":"blob","CharacterSetName":"NULL","CollationName":"NULL","NumbericScale":0,"IsBool":false,"Unsigned":false,"IsPrimary":false,"AutoIncrement":false,"EnumValues":[],"SetValues":[],"CharacterMaximumLength":65535,"NumbericPrecision":0,"Fsp":0,"IsNullable":"NO","Value":null},{"ColumnName":"testbit","ColumnKey":"","ColumnDefault":"","DataType":"bit","Extra":"","ColumnType":"bit(8)","CharacterSetName":"NULL","CollationName":"NULL","NumbericScale":0,"IsBool":false,"Unsigned":false,"IsPrimary":false,"AutoIncrement":false,"EnumValues":[],"SetValues":[],"CharacterMaximumLength":0,"NumbericPrecision":8,"Fsp":0,"IsNullable":"NO","Value":null},{"ColumnName":"testbool","ColumnKey":"","ColumnDefault":"0","DataType":"tinyint","Extra":"","ColumnType":"tinyint(1)","CharacterSetName":"NULL","CollationName":"NULL","NumbericScale":0,"IsBool":true,"Unsigned":false,"IsPrimary":false,"AutoIncrement":false,"EnumValues":[],"SetValues":[],"CharacterMaximumLength":0,"NumbericPrecision":3,"Fsp":0,"IsNullable":"NO","Value":null},{"ColumnName":"testmediumblob","ColumnKey":"","ColumnDefault":"NULL","DataType":"mediumblob","Extra":"","ColumnType":"mediumblob","CharacterSetName":"NULL","CollationName":"NULL","NumbericScale":0,"IsBool":false,"Unsigned":false,"IsPrimary":false,"AutoIncrement":false,"EnumValues":[],"SetValues":[],"CharacterMaximumLength":16777215,"NumbericPrecision":0,"Fsp":0,"IsNullable":"NO","Value":null},{"ColumnName":"testlongblob","ColumnKey":"","ColumnDefault":"NULL","DataType":"longblob","Extra":"","ColumnType":"longblob","CharacterSetName":"NULL","CollationName":"NULL","NumbericScale":0,"IsBool":false,"Unsigned":false,"IsPrimary":false,"AutoIncrement":false,"EnumValues":[],"SetValues":[],"CharacterMaximumLength":4294967295,"NumbericPrecision":0,"Fsp":0,"IsNullable":"NO","Value":null},{"ColumnName":"testtinyblob","ColumnKey":"","ColumnDefault":"NULL","DataType":"tinyblob","Extra":"","ColumnType":"tinyblob","CharacterSetName":"NULL","CollationName":"NULL","NumbericScale":0,"IsBool":false,"Unsigned":false,"IsPrimary":false,"AutoIncrement":false,"EnumValues":[],"SetValues":[],"CharacterMaximumLength":255,"NumbericPrecision":0,"Fsp":0,"IsNullable":"NO","Value":null},{"ColumnName":"test_unsinged_tinyint","ColumnKey":"","ColumnDefault":"1","DataType":"tinyint","Extra":"","ColumnType":"tinyint unsigned","CharacterSetName":"NULL","CollationName":"NULL","NumbericScale":0,"IsBool":false,"Unsigned":true,"IsPrimary":false,"AutoIncrement":false,"EnumValues":[],"SetValues":[],"CharacterMaximumLength":0,"NumbericPrecision":3,"Fsp":0,"IsNullable":"NO","Value":null},{"ColumnName":"test_unsinged_smallint","ColumnKey":"","ColumnDefault":"2","DataType":"smallint","Extra":"","ColumnType":"smallint unsigned","CharacterSetName":"NULL","CollationName":"NULL","NumbericScale":0,"IsBool":false,"Unsigned":true,"IsPrimary":false,"AutoIncrement":false,"EnumValues":[],"SetValues":[],"CharacterMaximumLength":0,"NumbericPrecision":5,"Fsp":0,"IsNullable":"NO","Value":null},{"ColumnName":"test_unsinged_mediumint","ColumnKey":"","ColumnDefault":"3","DataType":"mediumint","Extra":"","ColumnType":"mediumint unsigned","CharacterSetName":"NULL","CollationName":"NULL","NumbericScale":0,"IsBool":false,"Unsigned":true,"IsPrimary":false,"AutoIncrement":false,"EnumValues":[],"SetValues":[],"CharacterMaximumLength":0,"NumbericPrecision":7,"Fsp":0,"IsNullable":"NO","Value":null},{"ColumnName":"test_unsinged_int","ColumnKey":"","ColumnDefault":"4","DataType":"int","Extra":"","ColumnType":"int unsigned","CharacterSetName":"NULL","CollationName":"NULL","NumbericScale":0,"IsBool":false,"Unsigned":true,"IsPrimary":false,"AutoIncrement":false,"EnumValues":[],"SetValues":[],"CharacterMaximumLength":0,"NumbericPrecision":10,"Fsp":0,"IsNullable":"NO","Value":null},{"ColumnName":"test_unsinged_bigint","ColumnKey":"","ColumnDefault":"5","DataType":"bigint","Extra":"","ColumnType":"bigint unsigned","CharacterSetName":"NULL","CollationName":"NULL","NumbericScale":0,"IsBool":false,"Unsigned":true,"IsPrimary":false,"AutoIncrement":false,"EnumValues":[],"SetValues":[],"CharacterMaximumLength":0,"NumbericPrecision":20,"Fsp":0,"IsNullable":"NO","Value":null},{"ColumnName":"testtime2_1","ColumnKey":"","ColumnDefault":"00:00:00.0","DataType":"time","Extra":"","ColumnType":"time(1)","CharacterSetName":"NULL","CollationName":"NULL","NumbericScale":0,"IsBool":false,"Unsigned":false,"IsPrimary":false,"AutoIncrement":false,"EnumValues":[],"SetValues":[],"CharacterMaximumLength":0,"NumbericPrecision":0,"Fsp":1,"IsNullable":"NO","Value":null},{"ColumnName":"testtime2_2","ColumnKey":"","ColumnDefault":"00:00:00.00","DataType":"time","Extra":"","ColumnType":"time(2)","CharacterSetName":"NULL","CollationName":"NULL","NumbericScale":0,"IsBool":false,"Unsigned":false,"IsPrimary":false,"AutoIncrement":false,"EnumValues":[],"SetValues":[],"CharacterMaximumLength":0,"NumbericPrecision":0,"Fsp":2,"IsNullable":"NO","Value":null},{"ColumnName":"testtime2_3","ColumnKey":"","ColumnDefault":"00:00:00.000","DataType":"time","Extra":"","ColumnType":"time(3)","CharacterSetName":"NULL","CollationName":"NULL","NumbericScale":0,"IsBool":false,"Unsigned":false,"IsPrimary":false,"AutoIncrement":false,"EnumValues":[],"SetValues":[],"CharacterMaximumLength":0,"NumbericPrecision":0,"Fsp":3,"IsNullable":"NO","Value":null},{"ColumnName":"testtime2_4","ColumnKey":"","ColumnDefault":"00:00:00.0000","DataType":"time","Extra":"","ColumnType":"time(4)","CharacterSetName":"NULL","CollationName":"NULL","NumbericScale":0,"IsBool":false,"Unsigned":false,"IsPrimary":false,"AutoIncrement":false,"EnumValues":[],"SetValues":[],"CharacterMaximumLength":0,"NumbericPrecision":0,"Fsp":4,"IsNullable":"NO","Value":null},{"ColumnName":"testtime2_5","ColumnKey":"","ColumnDefault":"00:00:00.00000","DataType":"time","Extra":"","ColumnType":"time(5)","CharacterSetName":"NULL","CollationName":"NULL","NumbericScale":0,"IsBool":false,"Unsigned":false,"IsPrimary":false,"AutoIncrement":false,"EnumValues":[],"SetValues":[],"CharacterMaximumLength":0,"NumbericPrecision":0,"Fsp":5,"IsNullable":"NO","Value":null},{"ColumnName":"testtime2_6","ColumnKey":"","ColumnDefault":"00:00:00.000000","DataType":"time","Extra":"","ColumnType":"time(6)","CharacterSetName":"NULL","CollationName":"NULL","NumbericScale":0,"IsBool":false,"Unsigned":false,"IsPrimary":false,"AutoIncrement":false,"EnumValues":[],"SetValues":[],"CharacterMaximumLength":0,"NumbericPrecision":0,"Fsp":6,"IsNullable":"NO","Value":null},{"ColumnName":"testtimestamp2_1","ColumnKey":"","ColumnDefault":"CURRENT_TIMESTAMP(1)","DataType":"timestamp","Extra":"DEFAULT_GENERATED","ColumnType":"timestamp(1)","CharacterSetName":"NULL","CollationName":"NULL","NumbericScale":0,"IsBool":false,"Unsigned":false,"IsPrimary":false,"AutoIncrement":false,"EnumValues":[],"SetValues":[],"CharacterMaximumLength":0,"NumbericPrecision":0,"Fsp":1,"IsNullable":"NO","Value":null},{"ColumnName":"testtimestamp2_2","ColumnKey":"","ColumnDefault":"CURRENT_TIMESTAMP(2)","DataType":"timestamp","Extra":"DEFAULT_GENERATED","ColumnType":"timestamp(2)","CharacterSetName":"NULL","CollationName":"NULL","NumbericScale":0,"IsBool":false,"Unsigned":false,"IsPrimary":false,"AutoIncrement":false,"EnumValues":[],"SetValues":[],"CharacterMaximumLength":0,"NumbericPrecision":0,"Fsp":2,"IsNullable":"NO","Value":null},{"ColumnName":"testtimestamp2_3","ColumnKey":"","ColumnDefault":"CURRENT_TIMESTAMP(3)","DataType":"timestamp","Extra":"DEFAULT_GENERATED","ColumnType":"timestamp(3)","CharacterSetName":"NULL","CollationName":"NULL","NumbericScale":0,"IsBool":false,"Unsigned":false,"IsPrimary":false,"AutoIncrement":false,"EnumValues":[],"SetValues":[],"CharacterMaximumLength":0,"NumbericPrecision":0,"Fsp":3,"IsNullable":"NO","Value":null},{"ColumnName":"testtimestamp2_4","ColumnKey":"","ColumnDefault":"CURRENT_TIMESTAMP(4)","DataType":"timestamp","Extra":"DEFAULT_GENERATED","ColumnType":"timestamp(4)","CharacterSetName":"NULL","CollationName":"NULL","NumbericScale":0,"IsBool":false,"Unsigned":false,"IsPrimary":false,"AutoIncrement":false,"EnumValues":[],"SetValues":[],"CharacterMaximumLength":0,"NumbericPrecision":0,"Fsp":4,"IsNullable":"NO","Value":null},{"ColumnName":"testtimestamp2_5","ColumnKey":"","ColumnDefault":"CURRENT_TIMESTAMP(5)","DataType":"timestamp","Extra":"DEFAULT_GENERATED","ColumnType":"timestamp(5)","CharacterSetName":"NULL","CollationName":"NULL","NumbericScale":0,"IsBool":false,"Unsigned":false,"IsPrimary":false,"AutoIncrement":false,"EnumValues":[],"SetValues":[],"CharacterMaximumLength":0,"NumbericPrecision":0,"Fsp":5,"IsNullable":"NO","Value":null},{"ColumnName":"testtimestamp2_6","ColumnKey":"","ColumnDefault":"CURRENT_TIMESTAMP(6)","DataType":"timestamp","Extra":"DEFAULT_GENERATED","ColumnType":"timestamp(6)","CharacterSetName":"NULL","CollationName":"NULL","NumbericScale":0,"IsBool":false,"Unsigned":false,"IsPrimary":false,"AutoIncrement":false,"EnumValues":[],"SetValues":[],"CharacterMaximumLength":0,"NumbericPrecision":0,"Fsp":6,"IsNullable":"NO","Value":null},{"ColumnName":"testdatetime2_1","ColumnKey":"","ColumnDefault":"0000-00-00 00:00:00.0","DataType":"datetime","Extra":"","ColumnType":"datetime(1)","CharacterSetName":"NULL","CollationName":"NULL","NumbericScale":0,"IsBool":false,"Unsigned":false,"IsPrimary":false,"AutoIncrement":false,"EnumValues":[],"SetValues":[],"CharacterMaximumLength":0,"NumbericPrecision":0,"Fsp":1,"IsNullable":"NO","Value":null},{"ColumnName":"testdatetime2_2","ColumnKey":"","ColumnDefault":"0000-00-00 00:00:00.00","DataType":"datetime","Extra":"","ColumnType":"datetime(2)","CharacterSetName":"NULL","CollationName":"NULL","NumbericScale":0,"IsBool":false,"Unsigned":false,"IsPrimary":false,"AutoIncrement":false,"EnumValues":[],"SetValues":[],"CharacterMaximumLength":0,"NumbericPrecision":0,"Fsp":2,"IsNullable":"NO","Value":null},{"ColumnName":"testdatetime2_3","ColumnKey":"","ColumnDefault":"0000-00-00 00:00:00.000","DataType":"datetime","Extra":"","ColumnType":"datetime(3)","CharacterSetName":"NULL","CollationName":"NULL","NumbericScale":0,"IsBool":false,"Unsigned":false,"IsPrimary":false,"AutoIncrement":false,"EnumValues":[],"SetValues":[],"CharacterMaximumLength":0,"NumbericPrecision":0,"Fsp":3,"IsNullable":"NO","Value":null},{"ColumnName":"testdatetime2_4","ColumnKey":"","ColumnDefault":"0000-00-00 00:00:00.0000","DataType":"datetime","Extra":"","ColumnType":"datetime(4)","CharacterSetName":"NULL","CollationName":"NULL","NumbericScale":0,"IsBool":false,"Unsigned":false,"IsPrimary":false,"AutoIncrement":false,"EnumValues":[],"SetValues":[],"CharacterMaximumLength":0,"NumbericPrecision":0,"Fsp":4,"IsNullable":"NO","Value":null},{"ColumnName":"testdatetime2_5","ColumnKey":"","ColumnDefault":"0000-00-00 00:00:00.00000","DataType":"datetime","Extra":"","ColumnType":"datetime(5)","CharacterSetName":"NULL","CollationName":"NULL","NumbericScale":0,"IsBool":false,"Unsigned":false,"IsPrimary":false,"AutoIncrement":false,"EnumValues":[],"SetValues":[],"CharacterMaximumLength":0,"NumbericPrecision":0,"Fsp":5,"IsNullable":"NO","Value":null},{"ColumnName":"testdatetime2_6","ColumnKey":"","ColumnDefault":"0000-00-00 00:00:00.000000","DataType":"datetime","Extra":"","ColumnType":"datetime(6)","CharacterSetName":"NULL","CollationName":"NULL","NumbericScale":0,"IsBool":false,"Unsigned":false,"IsPrimary":false,"AutoIncrement":false,"EnumValues":[],"SetValues":[],"CharacterMaximumLength":0,"NumbericPrecision":0,"Fsp":6,"IsNullable":"NO","Value":null},{"ColumnName":"test_json","ColumnKey":"","ColumnDefault":"NULL","DataType":"json","Extra":"","ColumnType":"json","CharacterSetName":"NULL","CollationName":"NULL","NumbericScale":0,"IsBool":false,"Unsigned":false,"IsPrimary":false,"AutoIncrement":false,"EnumValues":[],"SetValues":[],"CharacterMaximumLength":0,"NumbericPrecision":0,"Fsp":0,"IsNullable":"YES","Value":null}]`

type Column struct {
	ColumnName             string
	ColumnKey              string
	ColumnDefault          string
	DataType               string
	Extra                  string
	ColumnType             string
	CharacterSetName       string
	CollationName          string
	NumbericScale          int
	IsBool                 bool
	Unsigned               bool
	IsPrimary              bool
	AutoIncrement          bool
	EnumValues             []string
	SetValues              []string
	CharacterMaximumLength int
	NumbericPrecision      int
	IsNullable             string
	Value                  interface{}
}

type EventType int8

const (
	RANDALL   EventType = -1
	INSERT    EventType = 0
	UPDATE    EventType = 1
	DELETE    EventType = 2
	SQLTYPE   EventType = 3
	COMMIT    EventType = 4
	OTHERTYPE EventType = 5
)

func GetRandomString(l int, cn int) string {
	str := "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ^&*'\";\\/%$#@90-_|<>?{}[]+.!~`,=0"
	str2Arr := []string{"测", "试", "数", "据"}
	bytes := []byte(str)
	result1 := []byte{}
	result2 := ""
	for i := 0; i < l; i++ {
		rand.Seed(time.Now().UnixNano() + int64(i))
		result1 = append(result1, bytes[rand.Intn(len(bytes))])
	}
	for i := 0; i < cn; i++ {
		rand.Seed(time.Now().UnixNano() + int64(i))
		result2 += str2Arr[rand.Intn(len(str2Arr))]
	}
	rand.Seed(time.Now().UnixNano())
	return string(result1) + result2
}

func GetTimeAndNsen(ColumnDataType string) string {
	ColumnDataType = strings.ToLower(ColumnDataType)
	var timeFormat string
	i := strings.Index(ColumnDataType, "(")
	var columnType string
	if i < 0 {
		columnType = ColumnDataType
	} else {
		columnType = ColumnDataType[0:i]
	}
	switch columnType {
	case "time":
		timeFormat = "15:03:04"
	case "timestamp", "datetime":
		timeFormat = "2006-01-02 15:03:04"
	case "year":
		timeFormat = "2006"
	default:
		return ""
	}
	var n = 0
	var err error
	if i > 0 {
		n, err = strconv.Atoi(ColumnDataType[i+1 : len(ColumnDataType)-1])
		if err != nil {
			panic(err.Error())
		}
	}
	if n > 0 {
		timeFormat += "." + fmt.Sprintf("%0*d", n, 0)
	}
	value := time.Now().Format(timeFormat)
	return value
}

type Event struct {
	Schema           string
	Talbe            string
	AutoIncrementNum uint64                            //自增字段值
	ColumnList       []*Column                         //字段属性列表
	position         uint32                            //位点
	dataMap          map[uint64]map[string]interface{} //随机生成的数据最终记录值,id为key
	idVal            uint64                            //随机生成数据的时候，指定的id值。随机生成一次数据后自动清0
	saveHistory      bool                              //是否保存历史生成的随机数据。假如一个id 有insert ,update 则只保存update之后的数据，假如后面又有delete了，则会被清除掉这个id数据
	isNull           bool                              // 是否生成null值的数据，默认为false
	ColumnMapping    map[string]string                 // 字段类型
}

func NewEvent() *Event {
	var data []*Column
	json.Unmarshal([]byte(columnJsonString), &data)
	event := &Event{
		Schema:           "bifrost_test",
		Talbe:            "binlog_field_test",
		AutoIncrementNum: 0,
		ColumnList:       data,
		position:         0,
		dataMap:          make(map[uint64]map[string]interface{}, 0),
		idVal:            0,
		saveHistory:      true,
		isNull:           false,
	}
	event.initTableColumnMapping()
	return event
}

func (This *Event) SetSchema(name string) *Event {
	This.Schema = name
	return This
}

func (This *Event) SetTable(name string) *Event {
	This.Talbe = name
	return This
}

//设置随机生成数据的id值
func (This *Event) SetIdVal(val interface{}) *Event {
	int64Val, err := strconv.ParseUint(fmt.Sprint(val), 10, 64)
	if err == nil {
		This.idVal = int64Val
	}
	return This
}

//设置是否要保存历史数据
func (This *Event) SetSaveHistory(b bool) *Event {
	This.saveHistory = b
	return This
}

//设置是否生成null值的数据
func (This *Event) SetIsNull(b bool) *Event {
	This.isNull = b
	return This
}

//保存随机生成的数据
func (This *Event) setDataToMap(data map[string]interface{}) {
	if This.saveHistory == false {
		return
	}
	int64Val, err := strconv.ParseUint(fmt.Sprint(data["id"]), 10, 64)
	if err == nil {
		This.dataMap[int64Val] = data
	}
}

//获取所有生成的数据结果
func (This *Event) GetDataMap() map[uint64]map[string]interface{} {
	return This.dataMap
}

//删除数据
func (This *Event) delDataFromMap(data map[string]interface{}) {
	int64Val, err := strconv.ParseUint(fmt.Sprint(data["id"]), 10, 64)
	if err == nil {
		delete(This.dataMap, int64Val)
	}
}

//随机或者指定id获取一条已经生成的数据
func (This *Event) getRandDataFromMap(id uint64) map[string]interface{} {
	if id == 0 {
		for _, v := range This.dataMap {
			return v
		}
	} else {
		if _, ok := This.dataMap[id]; ok {
			return This.dataMap[id]
		}
	}
	return nil
}

func (This *Event) GetTableColumnMapping() map[string]string {
	return This.ColumnMapping
}

func (This *Event) initTableColumnMapping() {
	ColumnMapping := make(map[string]string)
	for _, v := range This.ColumnList {
		var columnMappingType string
		switch v.DataType {
		case "tinyint":
			if v.Unsigned {
				columnMappingType = "uint8"
			} else {
				if v.ColumnType == "tinyint(1)" {
					columnMappingType = "bool"
				} else {
					columnMappingType = "int8"
				}
			}
		case "smallint":
			if v.Unsigned {
				columnMappingType = "uint16"
			} else {
				columnMappingType = "int16"
			}
		case "mediumint":
			if v.Unsigned {
				columnMappingType = "uint24"
			} else {
				columnMappingType = "int24"
			}
		case "int":
			if v.Unsigned {
				columnMappingType = "uint32"
			} else {
				columnMappingType = "int32"
			}
		case "bigint":
			if v.Unsigned {
				columnMappingType = "uint64"
			} else {
				columnMappingType = "int64"
			}
		case "numeric":
			columnMappingType = strings.Replace(v.ColumnType, "numeric", "decimal", 1)
		case "real":
			columnMappingType = strings.Replace(v.ColumnType, "real", "double", 1)
		default:
			columnMappingType = v.ColumnType
			break
		}
		if v.IsNullable == "YES" {
			ColumnMapping[v.ColumnName] = "Nullable(" + columnMappingType + ")"
		} else {
			ColumnMapping[v.ColumnName] = columnMappingType
		}
	}
	This.ColumnMapping = ColumnMapping
}

//随机生成数据
func (This *Event) getSchemaTableFieldAndVal(columnList []*Column, eventType EventType) ([]interface{}, map[string]interface{}) {
	data := make([]interface{}, 0)
	columnData := make(map[string]interface{}, 0)

	defer func() {
		This.setDataToMap(columnData)
	}()
	for _, columnType := range columnList {
		rand.Seed(time.Now().UnixNano())
		var randResult int
		if rand.Intn(2) >= 1 {
			randResult = 1
		} else {
			randResult = 0
		}
		var a uint64
		if columnType.AutoIncrement {
			if This.idVal == 0 {
				if eventType == INSERT {
					This.AutoIncrementNum++
				}
				a = This.AutoIncrementNum
			} else {
				a = This.idVal
			}
			//idVal 设置只能一次生效。每次生成数据之后，自动清0
			This.idVal = 0
			switch columnType.DataType {
			case "tinyint":
				if columnType.Unsigned == true {
					columnData[columnType.ColumnName] = uint8(a)
				} else {
					columnData[columnType.ColumnName] = int8(a)
				}
				break
			case "smallint":
				if columnType.Unsigned == true {
					columnData[columnType.ColumnName] = uint16(a)
				} else {
					columnData[columnType.ColumnName] = int16(a)
				}
				break
			case "mediumint", "int":
				if columnType.Unsigned == true {
					columnData[columnType.ColumnName] = uint32(a)
				} else {
					columnData[columnType.ColumnName] = int32(a)
				}
				break
			case "bigint":
				if columnType.Unsigned == true {
					columnData[columnType.ColumnName] = uint64(a)
				} else {
					columnData[columnType.ColumnName] = int64(a)
				}
				break
			}
			data = append(data, columnData[columnType.ColumnName])
			continue
		}
		if This.isNull {
			columnType.Value = nil
			data = append(data, nil)
			columnData[columnType.ColumnName] = columnType.Value
		} else {
			switch columnType.DataType {
			case "int", "tinyint", "smallint", "mediumint", "bigint":
				if columnType.IsBool {
					if randResult == 1 {
						data = append(data, "1")
						columnType.Value = true
					} else {
						data = append(data, "0")
						columnType.Value = false
					}
				} else {
					var Value interface{}
					switch columnType.DataType {
					case "tinyint":
						if columnType.Unsigned == true {
							Value = uint8(255)
						} else {
							if randResult == 1 {
								Value = int8(127)
							} else {
								Value = int8(-128)
							}
						}
						break
					case "smallint":
						if columnType.Unsigned == true {
							Value = uint16(65535)
						} else {
							if randResult == 1 {
								Value = int16(32767)
							} else {
								Value = int16(-32768)
							}
						}
						break
					case "mediumint":
						if columnType.Unsigned == true {
							Value = uint32(16777215)
						} else {
							if randResult == 1 {
								Value = int32(8388607)
							} else {
								Value = int32(-8388608)
							}
						}
						break
					case "int":
						if columnType.Unsigned == true {
							Value = uint32(4294967295)
						} else {
							if randResult == 1 {
								Value = int32(2147483647)
							} else {
								Value = int32(-2147483648)
							}
						}
						break
					case "bigint":
						if columnType.Unsigned == true {
							Value = uint64(18446744073709551615)
						} else {
							if randResult == 1 {
								Value = int64(9223372036854775807)
							} else {
								Value = int64(-9223372036854775808)
							}
						}
						break
					}
					columnType.Value = Value
					data = append(data, Value)
				}
				break
			case "char", "varchar":
				var enSize, cnSize int = 0, 0
				if strings.Contains(columnType.CharacterSetName, "utf") {
					if columnType.CharacterMaximumLength/4 > 0 {
						cnSize = rand.Intn(columnType.CharacterMaximumLength / 4)
						enSize = columnType.CharacterMaximumLength - cnSize*4
					} else {
						enSize = rand.Intn(columnType.CharacterMaximumLength - 1)
					}
				} else {
					enSize = rand.Intn(columnType.CharacterMaximumLength - 1)
				}
				Value := GetRandomString(enSize, cnSize)
				columnType.Value = Value
				data = append(data, Value)
				break
			case "tinytext", "tinyblob", "text", "mediumtext", "smalltext", "blob", "mediumblob", "smallblob", "longblob":
				var enSize, cnSize int = 0, 0
				rand.Seed(time.Now().UnixNano())

				var n int

				n = rand.Intn(255 / 4)

				if n == 0 {
					n = 1
				}
				if strings.Contains(columnType.CharacterSetName, "utf") {
					cnSize = rand.Intn(n)
				}
				enSize = n - cnSize
				Value := GetRandomString(enSize, cnSize)
				columnType.Value = Value
				data = append(data, Value)
				break
			case "year":
				Value := time.Now().Format("2006")
				columnType.Value = Value
				data = append(data, Value)
				break
			case "time":
				Value := GetTimeAndNsen(columnType.ColumnType)
				columnType.Value = Value
				data = append(data, Value)
				break
			case "date":
				Value := time.Now().Format("2006-01-02")
				columnType.Value = Value
				data = append(data, Value)
				break
			case "datetime", "timestamp":
				Value := GetTimeAndNsen(columnType.ColumnType)
				columnType.Value = Value
				data = append(data, Value)
				break
			case "bit":
				var Value int64 = 1
				if columnType.NumbericPrecision < 16 {
					Value = int64(rand.Intn(127))
				}
				if columnType.NumbericPrecision >= 16 && columnType.NumbericPrecision < 32 {
					Value = int64(rand.Intn(32767))
				}
				if columnType.NumbericPrecision >= 32 && columnType.NumbericPrecision < 64 {
					Value = int64(rand.Int31())
				}
				if columnType.NumbericPrecision == 64 {
					Value = rand.Int63()
				}
				columnType.Value = Value
				data = append(data, Value)
				break
			case "float":
				Value := strconv.FormatFloat(float64(rand.Float32()), 'f', 2, 32)
				Value2, _ := strconv.ParseFloat(Value, 32)
				f1 := float32(rand.Intn(999999))
				f2 := f1 + float32(Value2)
				if randResult == 1 {
					f2 = 0 - f2
				}
				columnType.Value = f2
				data = append(data, f2)
				break
			case "double":
				Value := strconv.FormatFloat(float64(rand.Float64()), 'f', 2, 64)
				Value2, _ := strconv.ParseFloat(Value, 64)
				f1 := float64(rand.Intn(999999))
				f2 := f1 + float64(Value2)
				if randResult == 1 {
					f2 = 0 - f2
				}
				columnType.Value = f2
				data = append(data, f2)
				break
			case "decimal":
				Value := strconv.FormatFloat(float64(rand.Float64()), 'f', 2, 64)
				Value2, _ := strconv.ParseFloat(Value, 64)
				f1 := float64(rand.Intn(999999))
				f2 := f1 + float64(Value2)
				if randResult == 1 {
					f2 = 0 - f2
				}
				f3 := strconv.FormatFloat(float64(rand.Float64()), 'f', 2, 64)
				columnType.Value = f3
				data = append(data, f3)
				break
			case "set":
				d := strings.Replace(columnType.ColumnType, "set(", "", -1)
				d = strings.Replace(d, ")", "", -1)
				d = strings.Replace(d, "'", "", -1)
				set_values := strings.Split(d, ",")
				Value := make([]string, 0)
				if len(set_values) > 1 {
					Value = append(Value, set_values[0])
					Value = append(Value, set_values[len(set_values)-1])
				} else {
					Value = append(Value, set_values[0])
				}
				columnType.Value = Value
				data = append(data, strings.Replace(strings.Trim(fmt.Sprint(Value), "[]"), " ", ",", -1))
				break
			case "enum":
				d := strings.Replace(columnType.ColumnType, "enum(", "", -1)
				d = strings.Replace(d, ")", "", -1)
				d = strings.Replace(d, "'", "", -1)
				enum_values := strings.Split(d, ",")
				Value := enum_values[rand.Intn(len(enum_values)-1)]
				columnType.Value = Value
				data = append(data, Value)
				break
			case "json":
				Value := This.GetJsonData()
				columnType.Value = Value
				data = append(data, Value)
				break
			default:
				data = append(data, "0")
				break
			}

			columnData[columnType.ColumnName] = columnType.Value
		}
	}
	//log.Println("This.lastEventData:",This.lastEventData)
	return data, columnData
}

func GetString() string {
	return GetRandomString(11, 20)
}
func GetTimeString() string {
	return time.Now().Format("2006-01-02 15:04:05")
}
func GetNull() interface{} {
	return nil
}
func GetInt32() int32 {
	return int32(-2147483648)
}
func GetUint32() uint32 {
	return uint32(4294967295)
}

func GetInt64() int64 {
	return int64(-9223372036854775808)
}

func GetUint64() uint64 {
	return uint64(18446744073709551615)
}

func GetBool() bool {
	if (time.Now().Unix() & 2) == 0 {
		return true
	}
	return false
}
func GetFloat64() float64 {
	Value := strconv.FormatFloat(float64(rand.Float64()), 'f', 2, 64)
	Value2, _ := strconv.ParseFloat(Value, 64)
	f1 := float64(rand.Intn(999999))
	f2 := f1 + float64(Value2)
	if (time.Now().Unix() & 2) == 0 {
		f2 = 0 - f2
	}
	return f2
}

func (This *Event) GetJsonData() map[string][]map[string]interface{} {
	m := make(map[string][]map[string]interface{}, 0)
	m["testK"] = make([]map[string]interface{}, 1)
	m["testK"][0] = make(map[string]interface{}, 0)
	m["testK"][0]["String"] = GetString()
	m["testK"][0]["Null"] = GetNull()
	m["testK"][0]["Time"] = GetTimeString()
	m["testK"][0]["Int32"] = GetInt32()
	m["testK"][0]["Uint32"] = GetUint32()
	m["testK"][0]["Int64"] = GetInt64()
	m["testK"][0]["Uint64"] = GetUint64()
	m["testK"][0]["Uint64"] = GetUint64()
	m["testK"][0]["Bool"] = GetBool()
	m["testK"][0]["Float64"] = GetFloat64()
	return m
}

func (This *Event) GetPri() []string {
	var id string = "id"
	Pri := make([]string, 1)
	Pri[0] = id
	return Pri
}

func (This *Event) GetTestInsertData() *pluginDriver.PluginDataType {
	Rows := make([]map[string]interface{}, 1)

	_, Rows[0] = This.getSchemaTableFieldAndVal(This.ColumnList, INSERT)

	This.position += 100
	return &pluginDriver.PluginDataType{
		Timestamp:      uint32(time.Now().Unix()),
		EventType:      "insert",
		Rows:           Rows,
		Query:          "",
		SchemaName:     This.Schema,
		TableName:      This.Talbe,
		BinlogFileNum:  10,
		BinlogPosition: This.position,
		Pri:            This.GetPri(),
		ColumnMapping:  This.GetTableColumnMapping(),
	}
}

func (This *Event) deepCopy(dst, src interface{}) error {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(src); err != nil {
		return err
	}
	return gob.NewDecoder(bytes.NewBuffer(buf.Bytes())).Decode(dst)
}

func (This *Event) GetTestUpdateData() *pluginDriver.PluginDataType {
	Rows := make([]map[string]interface{}, 2)

	//随机或者指定一个id获取一条随机生成的数据。用作为旧数据
	var m map[string]interface{}
	if This.idVal == 0 {
		m = This.getRandDataFromMap(0)
		if m == nil {
			_, m = This.getSchemaTableFieldAndVal(This.ColumnList, INSERT)
		}
	} else {
		m = This.getRandDataFromMap(This.idVal)
	}

	//指定一个id值，再去获取随机生成的更新数据
	This.SetIdVal(m["id"])

	Rows[0] = m
	_, Rows[1] = This.getSchemaTableFieldAndVal(This.ColumnList, UPDATE)

	This.position += 100
	return &pluginDriver.PluginDataType{
		Timestamp:      uint32(time.Now().Unix()),
		EventType:      "update",
		Rows:           Rows,
		Query:          "",
		SchemaName:     This.Schema,
		TableName:      This.Talbe,
		BinlogFileNum:  10,
		BinlogPosition: This.position,
		Pri:            This.GetPri(),
		ColumnMapping:  This.GetTableColumnMapping(),
	}
}

func (This *Event) GetTestDeleteData() *pluginDriver.PluginDataType {
	Rows := make([]map[string]interface{}, 1)

	var m map[string]interface{}
	if This.idVal == 0 {
		m = This.getRandDataFromMap(0)
		if m == nil {
			_, m = This.getSchemaTableFieldAndVal(This.ColumnList, INSERT)
		}
	} else {
		m = This.getRandDataFromMap(This.idVal)
	}

	//从数据中删除这条数据
	This.delDataFromMap(m)

	Rows[0] = m

	This.position += 100
	return &pluginDriver.PluginDataType{
		Timestamp:      uint32(time.Now().Unix()),
		EventType:      "delete",
		Rows:           Rows,
		Query:          "",
		SchemaName:     This.Schema,
		TableName:      This.Talbe,
		BinlogFileNum:  10,
		BinlogPosition: This.position,
		Pri:            This.GetPri(),
		ColumnMapping:  This.GetTableColumnMapping(),
	}
}

func (This *Event) GetTestQueryData() *pluginDriver.PluginDataType {
	var Rows []map[string]interface{}
	Rows = make([]map[string]interface{}, 0)

	This.position += 100
	return &pluginDriver.PluginDataType{
		Timestamp:      uint32(time.Now().Unix()),
		EventType:      "sql",
		Rows:           Rows,
		Query:          "ALTER TABLE `" + This.Schema + "`.`" + This.Talbe + "` CHANGE COLUMN `testvarchar` `testvarchar` varchar(255) NOT NULL",
		SchemaName:     This.Schema,
		TableName:      This.Talbe,
		BinlogFileNum:  10,
		BinlogPosition: This.position,
	}
}

func (This *Event) GetTestCommitData() *pluginDriver.PluginDataType {
	var Rows []map[string]interface{}
	Rows = make([]map[string]interface{}, 0)

	This.position += 100
	return &pluginDriver.PluginDataType{
		Timestamp:      uint32(time.Now().Unix()),
		EventType:      "commit",
		Rows:           Rows,
		Query:          "COMMIT",
		SchemaName:     This.Schema,
		TableName:      This.Talbe,
		BinlogFileNum:  10,
		BinlogPosition: This.position,
	}
}
