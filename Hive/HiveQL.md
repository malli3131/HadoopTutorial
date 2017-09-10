```
1. show databases;
2. create database hadoop;
3. show databases;
4. create database india;
5. use hadoop;
6. create table people(name string);
7. select * from people;
8. show databases like 's.*';
9. create schema catalog;
10. show databases;
11. create database catalog;
12. create database if not exists catalog;
13. create database if not exists inventory COMMENT 'This is the database for product inventory';
14. create database if not exists ebay COMMENT 'This is the database for Ebay data' LOCATION '/ebay/';
15. use ebay;
16. create table product(name string);
17. create database if not exists target COMMENT 'This is the database for Target data' LOCATION '/target.db/' with dbproperties('created by' = 'XYZ', 'create_date'='10th Sept 2017');
use ebay;
use target;
show tables;
use target;
use ebay;
show tables;
select * from ebay.product;
;
drop database target;
show databases;
set hive.cli.current.db=true;
set hive.cli.currentdb=true;
set
;
set hive.cli.print.current.db=true;
select * from product;
set hive.cli.print.header=true;
select * from product;
describe product;
describe extended product;
describe formatted product;
describe ebay;
drop database target;
drop database if exists target;
drop database if exists ebay;
drop database if exists ebay restrict;
drop database if exists ebay cascade;
use ebay;
use default;
;
alter database catalog set DBProperties ('created' = 'hari');
show tables;
create table product;
create table product(name string);
create table if not exists product(name string);
create table if not exists store(name string comment 'Store name', place string comment 'Location of store');
describe product;
describe store;
create table if not exists emp(name string comment 'Employee name', age int comment 'Age of Employee', role string) comment 'Employee Table';
describe emp;
describe formatted emp;
load data local inpath '/home/ajith/bigdata/jobs/hive/emp' into table emp;
select * from emp;
drop table emp;
create table if not exists emp(name string comment 'Employee name', age int comment 'Age of Employee', role string) comment 'Employee Table' row format delimited fields terminated by '\t';
load data local inpath '/home/ajith/bigdata/jobs/hive/emp' into table emp;
select * from emp;
create table if not exists emp(name string comment 'Employee name', age int comment 'Age of Employee', role string) comment 'Employee Table' row format delimited fields terminated by '\t' stored as textfile;
drop table emp;
create table if not exists emp(name string comment 'Employee name', age int comment 'Age of Employee', role string) comment 'Employee Table' row format delimited fields terminated by '\t' stored as textfile;
load data local inpath '/home/ajith/bigdata/jobs/hive/emp' into table emp;
select * from emp;
create external table if not exists ext_emp(name string comment 'Employee name', age int comment 'Age of Employee', role string) comment 'Employee Table' row format delimited fields terminated by '\t' stored as textfile;
select * from emp;
select * from ext_emp;
load data local inpath '/home/ajith/bigdata/jobs/hive/emp' into table ext_emp;
select * from ext_emp;
drop table emp;
drop table ext_emp;
create external table if not exists ext_emp(name string comment 'Employee name', age int comment 'Age of Employee', role string) comment 'Employee Table' row format delimited fields terminated by '\t' stored as textfile;
select * from ext_emp;
drop table ext_emp;
create external table if not exists ext_emp(name string comment 'Employee name', age int comment 'Age of Employee', role string, salry int) comment 'Employee Table' row format delimited fields terminated by '\t' stored as textfile;
select * from ext_emp;
drop table ext_emp;
create external table if not exists ext_emp(name string comment 'Employee name', age int comment 'Age of Employee') comment 'Employee Table' row format delimited fields terminated by '\t' stored as textfile;
select * from ext_emp;
drop table ext_emp;
create external table if not exists ext_emp(name int comment 'Employee name', age int comment 'Age of Employee') comment 'Employee Table' row format delimited fields terminated by '\t' stored as textfile;
select * from ext_emp;
create table if not exists emp(name string comment 'Employee name', age int comment 'Age of Employee', role string) comment 'Employee Table' row format delimited fields terminated by '\t' stored as textfile;
create external table if not exists ext_emp(name int comment 'Employee name', age int comment 'Age of Employee') comment 'Employee Table' row format delimited fields terminated by '\t' stored as textfile;
show tables;
describe formatted emp;
describe formatted ext_emp;
describe extended ext_emp;
describe formatted ext_emp;
show tables;
exit
;
create temporary table if not exists tmp(name string);
show tables;
exit
;
show tables;
create external table nyse(
exchange varchar(20) comment 'market name',
stock string comment 'stock code',
sdate date comment 'trading date',
open double comment 'opening value',
high double comment 'day high value',
low double comment 'day low value',
close double comment 'closing value', 
volume bigint comment 'shares exchanged',
adj_close double comment 'previous day close')
comment 'Stocks table data for NYSE market'
row format delimited fields terminated by '\t'
store as textfile
location '/market/nyse'
tblproperties ('created' = 'naga', 'date' = '10th Sept 2017');
create external table nyse(
market string comment 'market name',
stock string comment 'stock code',
sdate date comment 'trading date',
open double comment 'opening value',
high double comment 'day high value',
low double comment 'day low value',
close double comment 'closing value', 
volume bigint comment 'shares exchanged',
adj_close double comment 'previous day close')
comment 'Stocks table data for NYSE market'
row format delimited fields terminated by '\t'
store as textfile
location '/market/nyse'
tblproperties ('created' = 'naga', 'date' = '10th Sept 2017');
create external table nyse(
market string comment 'market name',
stock string comment 'stock code',
sdate date comment 'trading date',
open double comment 'opening value',
high double comment 'day high value',
low double comment 'day low value',
close double comment 'closing value', 
volume bigint comment 'shares exchanged',
adj_close double comment 'previous day close')
comment 'Stocks table data for NYSE market'
row format delimited fields terminated by '\t'
stored as textfile
location '/market/nyse'
tblproperties ('created' = 'naga', 'date' = '10th Sept 2017');
select * from nyse limit 10;
drop table nyse;
create external table nyse(
market varchar(30) comment 'market name',
stock string comment 'stock code',
sdate date comment 'trading date',
open double comment 'opening value',
high double comment 'day high value',
low double comment 'day low value',
close double comment 'closing value', 
volume bigint comment 'shares exchanged',
adj_close double comment 'previous day close')
comment 'Stocks table data for NYSE market'
row format delimited fields terminated by '\t'
stored as textfile
location '/market/nyse'
tblproperties ('created' = 'naga', 'date' = '10th Sept 2017');
create external table  stocks as select stock, volume from nyse;
create table  stocks as select stock, volume from nyse;
select * from stocks limit 10;
drop table stocks;
create table  stocks row format delimited fields terminated by ',' as select stock, volume from nyse;
drop table stocks;
create table  stocks stored as parquetfile as select stock, volume from nyse;
drop table stocks;
create table  stocks stored as avrofile as select stock, volume from nyse;
describe formatted stocks;
create external table employee(name string, age int, place string, skills ARRAY<STRING>, address STRUCT<dno:String,floor:String,area:String,city:String,state:String,pin:bigint>) row format delimited fields terminated by '\t';
create external table employee(name string, age int, place string, skills ARRAY<STRING>, address STRUCT<dno:String,flr:String,area:String,city:String,state:String,pin:bigint>) row format delimited fields terminated by '\t';
load data local inpath '/home/ajith/bigdata/jobs/hive/emp' into table employee;
select * from employee;
drop table employee;
create external table employee(name string, age int, place string, skills ARRAY<STRING>, address STRUCT<dno:String,flr:String,area:String,city:String,state:String,pin:bigint>) row format delimited fields terminated by '\t' collection items terminated by ',';
select * from employee;
select skills from employee;
select skills[0] from employee;
select skills[0] as primary from employee;
select skills[0] as primaryskill from employee;
select skills[1] from employee;
select address.city from employee;
drop table employee;
create external table employee(name string, age int, place string, skills ARRAY<STRING>, address STRUCT<dno:String,flr:String,area:String,city:String,state:String,pin:bigint>, salary Map<String,bigint>) row format delimited fields terminated by '\t' collection items terminated by ',' map keys terminated by '#';
select * from employee;
select salary from employee;
select salary["basic"] from employee;
create table names(name string) row format delimited lines terminated by '|';
create table names(name string);
select * from names;
load data local inpath '/home/ajith/bigdata/jobs/hive/people' into table names;
select * from names;
select explode(split(name, '|'))) as name from names;
select explode(split(name, '|')) as name from names;
select explode(split(name, '\\|')) as name from names;
drop table names;
clear
;
describe stocks;
describe nyse;
select * from nyse limit 10;
create table sample like nyse;
describe sample;
select * from sample;
drop table emp;
show tables;
drop table if exists stocks purge;
drop table if exists product;
select * from nyse limit 10;
describe formatted nyse;
truncate table nyse;
show tables;
select * from employee;
describe formatted employee;
select * from employee;
create table names as select name from employee;
describe formatted names;
select * from names;
truncate table names;
select * from names;
describe formatted names;
describe nyse;
alter table nyse rename sdate to stockdate;
alter table nyse rename to nyse_stocks;
show tables;
describe formatted nyse_stocks;
alter table nyse_stocks set tblproperties ('place' = 'Bangalore');
describe formatted nyse_stocks;
alter table nyse_stocks change column sdate stockdate date;
describe nyse_stocks;
alter table nyse_stocks change column stockdate sdate string;
describe nyse_stocks;
alter table nyse_stocks change column sdate stockdate date;
describe nyse_stocks;
create table people(sno int, name string, place string) row format delimited fields terminated by '|';
drop table people;
create table people(sno int, name string, place string) row format delimited fields terminated by '|';
select * from people;
describe  people;
load data local inpath '/home/ajith/bigdata/jobs/hive/people' into table people;
select * from people;
load data local inpath '/home/ajith/bigdata/jobs/hive/people' into table people;
select * from people;
load data local inpath '/home/ajith/bigdata/jobs/hive/people' overwrite into table people;
select * from people;
truncate table people;
select * from people;
truncate table people;
load data inpath '/people' into table people;
select * from people;
show tables;
select * from volume;
describe volume;
truncate table volume;
select * from volume;
create table apache(pid int, pname string, company string) row format delimited fields terminated by ',';
select * from apache;
show tables;
select * from apache;
alter table apache set TBLPROPERTIES ("transactional"="true");
show tables;
select * from employee;
select * from people;
select * from nyse_stocks;
show tables;
create table stocks(stock string);
select * from stocks;
create table day_change(stock string, sdate date, change double) stored as sequencefile;
describe formatted day_change;
--insert into table day_change select stock, 
describe nyse_stocks;
insert into table day_change select stock, sdate, (close - open) from nyse_stocks;
select * from day_change;
insert overwrite into directory '/pig/people' select * from people;
insert into overwrite directory '/pig/people' select * from people;
insert overwrite directory '/pig/people' select * from people;
insert overwrite local directory '/home/ajith/people' select * from people;
select stock, sum(volume) as aggvolume from nyse_stocks group by stock;
select stock, sum(volume) as aggvolume from nyse_stocks group by stock order by aggvolume desc;
select stock, sum(volume) as aggvolume from nyse_stocks group by stock order by aggvolume desc limit 10;
select * from people;
describe people;
insert into table people (sno, name, place) values(3, 'siva', 'mysore');
describe people;
select * from people;
create table part_people(sno int, name string, age int) partitioned by (place string) row format delimited fields terminated by '|';
describe part_people;
describe formatted part_people;
load data local inpath '/home/ajith/bigdata/jobs/hive/people' into table part_people partition(place = 'Bangalore');
describe formatted part_people;
select * from part_people;
select * from part_people where place='Bangalore';
load data local inpath '/home/ajith/bigdata/jobs/hive/people' into table part_people partition(place = 'Hyderabad');
select * from part_people;
select * from part_people where place='Bangalore';
select * from part_people where place='Hyderabad';
select * from people;
insert into part_people partition(place = 'mysore') select sno, name where place='mysore';
describe people
;
insert into part_people partition(place = 'mysore') select sno, name from people where place='mysore';
insert into part_people partition(place = 'mysore') select sno, name, sno from people where place='mysore';
select * from part_people;
select * from part_people where place='mysore';
select * from part_people;
select * from part_people where place='mysore';
select * from part_people where place='Hyderabad';
create table part_stocks(market string, sdate date, open double, high double, low double, close double, volume bigint, adj_close double) partitioned by (stock string) row format delimited fields terminated by '\t';
insert into table part_stocks select market, sdate, open, high, low, close, volume, adj_close, stock from nyse_stocks;
insert into table part_stocks partition(stock) select market, sdate, open, high, low, close, volume, adj_close, stock from nyse_stocks;
set hive.exec.dynamic.partition.mode=nonstrict;
insert into table part_stocks partition(stock) select market, sdate, open, high, low, close, volume, adj_close, stock from nyse_stocks;
set hive.exec.max.dynamic.partitions.pernode=300;
insert into table part_stocks partition(stock) select market, sdate, open, high, low, close, volume, adj_close, stock from nyse_stocks;
select * from part_stocks;
select * from part_stocks where stock = 'CLI'
;
select * from nyse_stocks where stock = 'CLI'
;
select * from part_stocks where stock = 'CLI';
select * from part_stocks where stock = 'CA';
select * from nyse_stocks where stock = 'CA'
;
set hive.exec.dynamic.partition.mode=strict;
create table dsp_stocks(sdate date, open double, high double, low double, close double, volume bigint, adj_close double) partitioned by (market string, stock string) row format delimited fields terminated by ',';
insert into dsp_stocks partition(market='NYSE', stock) select sdate open, high, low, close, volume, adj_close, stock from nyse_stocks;
insert into dsp_stocks partition(market='NYSE', stock) select sdate, open, high, low, close, volume, adj_close, stock from nyse_stocks;
select * from dsp_stocks where market='NYSE' and stock = 'CLI';
insert into dsp_stocks partition(market='NYSE', stock='CLI') select sdate, open, high, low, close, volume, adj_close from nyse_stocks where market='NYSE' and stock ='CLI';
CREATE TABLE bucketed_stocks( market string, stock string, sdate date, sopen double, shigh double, slow double, sclose double, volume bigint, ad_close double) COMMENT 'A bucketed sorted stocks table' CLUSTERED BY (stock) SORTED BY (volume) INTO 5 BUCKETS STORED AS textfile;
insert into bucketed_stocks select * from nyse_stocks;
CREATE TABLE part_bucketed_stocks( market string, stock string, sopen double, shigh double, slow double, sclose double, volume bigint, ad_close double) COMMENT 'A partitioning bucketing stocks table' partitioned by (sdate date) CLUSTERED BY (stock) SORTED BY (volume) INTO 5 BUCKETS STORED AS textfile;
insert into part_bucketed_stocks partition(sdate) select market, stock, open, high, low, close, volume, adj_close, sdate from nyse_stocks;
set hive.exec.dynamic.partition.mode=nonstrict;
insert into part_bucketed_stocks partition(sdate) select market, stock, open, high, low, close, volume, adj_close, sdate from nyse_stocks;
;
create view stock_open as select stock, open from nyse_stocks;
show tables;
describe stock_open;
describe formatted stock_open;
describe formatted part_buckted_stocks;
show tables;
describe formatted part_bucketed_stocks;
select * from stock_open;
exit
;
describe formatted nyse_stocks;
CREATE INDEX nyse_stocks_index ON TABLE nyse_stocks (stock) AS 'COMPACT';
CREATE INDEX nyse_stocks_index ON TABLE nyse_stocks (stock) AS 'COMPACT' WITH DEFERRED REBUILD;
show tables;
select * from sample;
describe sample;
CREATE INDEX sample_index ON TABLE sample (stock) AS 'COMPACT' WITH DEFERRED REBUILD;
insert into table sample select * from nyse_stocks;
select * from nyse_stocks limit 10;
showfunctions;
show functions;
select month(sdate) from nyse_stocks limit 10;
add
;
add jar /home/ajith/bigdata/jobs/hive/month.jar;
list jar;
delete jar /home/ajith/bigdata/jobs/hive/month.jar;
list jar;
add jar /home/ajith/bigdata/jobs/hive/month.jar;
create temporary function GetMonth as 'com.hive.udf.GetMonth';
create temporary function GetMonth as 'com.hive.pdf.GetMonth';
show functions;
select getmonth(sdate) from nyse_stocks;
select getmonth(sdate) as month, sum(volume) as aggvolume from nyse_stocks group by month;
select getmonth(sdate) as month, volume from nyse_stocks;
select smp.month, sum(smp.volume) from (select getmonth(sdate) as month, volume from nyse_stocks) smp group by smp.month;
describe nyse_stocks;
select stock from nyse_stocks;
select lower(stock) as stock from nyse_stocks;
show functions;
```
