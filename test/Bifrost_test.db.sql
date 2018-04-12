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

/*Table structure for table `test1` */

DROP TABLE IF EXISTS `test1`;

CREATE TABLE `test1` (
  `id` int(11) unsigned NOT NULL AUTO_INCREMENT,
  `testtinyint` tinyint(4) NOT NULL DEFAULT '0',
  `testsmallint` smallint(6) NOT NULL DEFAULT '0',
  `testmediumint` mediumint(8) NOT NULL DEFAULT '0',
  `testint` int(11) NOT NULL DEFAULT '0',
  `testbigint` bigint(20) NOT NULL DEFAULT '0',
  `testvarchar` varchar(10) NOT NULL DEFAULT '',
  `testchar` char(2) NOT NULL DEFAULT '',
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
  PRIMARY KEY (`id`)
) ENGINE=MyISAM DEFAULT CHARSET=utf8;

/*Data for the table `test1` */

/*!40101 SET SQL_MODE=@OLD_SQL_MODE */;
/*!40014 SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS */;
/*!40014 SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS */;
/*!40111 SET SQL_NOTES=@OLD_SQL_NOTES */;
