<?php

$jsonString = '{"id":2,"test_unsinged_bigint":18446744073709551615,"test_unsinged_int":4294967295,"test_unsinged_mediumint":16777215,"test_unsinged_smallint":65535,"test_unsinged_tinyint":255,"testbigint":9223372036854775807,"testbit":73,"testblob":"]zE*s/,$+ypf9","testbool":true,"testchar":"","testdate":"2019-07-24","testdatetime":"2019-07-24 09:47:30","testdecimal":"-381526.6","testdouble":-381526.6,"testenum":"en2","testfloat":-381526.6,"testint":2147483647,"testlongblob":"$1AsX9$EURqYS2H#Qeq[Z7alM0nIR]2R","testmediumblob":"$1AsX9$EURqYS2H#Qeq[Z7alM0nIR]2R","testmediumint":8388607,"testset":["set1","set3"],"testsmallint":32767,"testtext":"]zE*s/试据数测数据试","testtime":"09:47:30","testtimestamp":"2019-07-24 09:47:30","testtinyblob":"$1AsX9$EURqYS2H#Qeq[Z7alM0nIR]2R","testtinyint":127,"testvarchar":"5!o/#\u0026$|zt","testyear":"2019"}';


$data = json_decode($jsonString,true);

var_dump($data);