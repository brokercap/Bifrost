/*
SQLyog Ultimate v10.42 
MySQL - 5.5.42-log : Database - jc3wish_test
*********************************************************************
*/


/*!40101 SET NAMES utf8 */;

/*!40101 SET SQL_MODE=''*/;

/*!40014 SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0 */;
/*!40014 SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0 */;
/*!40101 SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='NO_AUTO_VALUE_ON_ZERO' */;
/*!40111 SET @OLD_SQL_NOTES=@@SQL_NOTES, SQL_NOTES=0 */;
CREATE DATABASE /*!32312 IF NOT EXISTS*/`jc3wish_test` /*!40100 DEFAULT CHARACTER SET utf8 */;

USE `jc3wish_test`;

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

insert  into `binlog_field_test`(`id`,`testtinyint`,`testsmallint`,`testmediumint`,`testint`,`testbigint`,`testvarchar`,`testchar`,`testenum`,`testset`,`testtime`,`testdate`,`testyear`,`testtimestamp`,`testdatetime`,`testfloat`,`testdouble`,`testdecimal`,`testtext`,`testblob`,`testbit`,`testbool`,`testmediumblob`,`testlongblob`,`testtinyblob`,`test_unsinged_tinyint`,`test_unsinged_smallint`,`test_unsinged_mediumint`,`test_unsinged_int`,`test_unsinged_bigint`) values (1,-1,-2,-3,-4,-5,'testvarcha','te','en2','set1,set3','15:39:59','2018-05-08',2018,'2018-05-08 15:30:21','2018-05-08 15:30:21',9.39,9.39,9.39,'testtext','testblob','',1,'testmediumblob','testlongblob','testtinyblob',1,2,3,4,5);

/*!40101 SET SQL_MODE=@OLD_SQL_MODE */;
/*!40014 SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS */;
/*!40014 SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS */;
/*!40111 SET SQL_NOTES=@OLD_SQL_NOTES */;
