--hive 
create table iris(Sepal_Length double,Sepal_Width double,Petal_Length double,Petal_Width double,Species string) row format delimited fields terminated by ',';
--在把hdfs上数据迁移到hive中的表时，若出现数据位NULL，是因为没有指定列分隔符。需要在创建表格时指定数据的分割符号
load data local inpath '/home/dsg/desk/iris.txt' into table iris; 
--直接插入
load data local inpath '/home/dsg/desk/iris.txt' OVERWRITE INTO TABLE iris;
--覆盖之前内容插入
TRUNCATE TABLE iris;
--清空表内容
drop table iris;
select a from table sort by a limit 10;
--输出前10条信息








#创建新表

hive> CREATE TABLE t_hive (a int, b int, c int) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t';

#导入数据t_hive.txt到t_hive表  ,overwrite表示覆盖，into表示插入

hive> LOAD DATA LOCAL INPATH '/home/cos/demo/t_hive.txt' OVERWRITE INTO TABLE t_hive ;

#正则匹配表名

hive>show tables '*t*';

#增加一个字段

hive> ALTER TABLE t_hive ADD COLUMNS (new_col String);

#重命令表名

hive> ALTER TABLE t_hive RENAME TO t_hadoop;

#从HDFS加载数据

hive> LOAD DATA INPATH '/user/hive/warehouse/t_hive/t_hive.txt' OVERWRITE INTO TABLE t_hive2;

#从其他表导入数据

hive> INSERT OVERWRITE TABLE t_hive2 SELECT * FROM t_hive ;

#创建表并从其他表导入数据

hive> CREATE TABLE t_hive AS SELECT * FROM t_hive2 ;

#仅复制表结构不导数据

hive> CREATE TABLE t_hive3 LIKE t_hive;

#通过Hive导出到本地文件系统

hive> INSERT OVERWRITE LOCAL DIRECTORY '/tmp/t_hive' SELECT * FROM t_hive;

#Hive查询HiveQL

from ( select b,c as c2 from t_hive) t select t.b, t.c2 limit 2;

select b,c from t_hive limit 2;

#创建视图

hive> CREATE VIEW v_hive AS SELECT a,b FROM t_hive;

#删表

drop table if exists t_hft;

#创建分区表

DROP TABLE IF EXISTS t_hft;
CREATE TABLE t_hft(
SecurityID STRING,
tradeTime STRING,
PreClosePx DOUBLE
) PARTITIONED BY (tradeDate INT)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';

#导入分区数据

hive> load data local inpath '/home/BlueBreeze/data/t_hft_1.csv' overwrite into table t_hft partition(tradeDate=20130627);

#查看分区表

hive> SHOW PARTITIONS t_hft;


