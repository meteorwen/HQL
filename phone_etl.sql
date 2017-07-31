#hive：
create EXTERNAL table mphone(
id int,
STREAMNUMBER string,
SERVICEKEY string,
CALLCOST string,
CALLEDPARTYNUMBER string,
CALLINGPARTYNUMBER string,
CHARGEMODE string,
SPECIFICCHARGEDPAR string,
TRANSLATEDNUMBER string,
STARTDATEANDTIME string,
STOPDATEANDTIME string,
DURATION int,
CHARGECLASS string,
TRANSPARENTPARAMET string,
CALLTYPE int,
CALLERSUBGROUP string,
CALLEESUBGROUP string,
ORICALLEDNUMBER string,
ORICALLINGNUMBER string,
CALLERPNP string,
CALLEEPNP string,
REROUTE string,
ACRCALLID string,
GROUPNUMBER string,
CALLCATEGORY int,
CHARGETYPE string,
USERPIN string,
ACRTYPE int,
VIDEOCALLFLAG int,
SERVICEID string,
FORWARDNUMBER string,
EXTFORWARDNUMBER string,
SRFMSGID string,
MSSERVER string,
BEGBIGINTIME string,
RELEASECAUSE string,
RELEASEREASON string,
AREANUMBER string)
row format delimited fields terminated by '	' 
stored as textfile 
location '/user/root/data/phone';





#mysql:
create table test(
id BIGINT,
STREAMNUMBER varchar(255),
SERVICEKEY  varchar(255),
CALLCOST  varchar(255),
CALLEDPARTYNUMBER  varchar(255),
CALLINGPARTYNUMBER  varchar(255),
CHARGEMODE  varchar(255),
SPECIFICCHARGEDPAR  varchar(255),
TRANSLATEDNUMBER  varchar(255),
STARTDATEANDTIME  varchar(255),
STOPDATEANDTIME  varchar(255),
DURATION  BIGINT,
CHARGECLASS  varchar(255),
TRANSPARENTPARAMET  varchar(100),
CALLTYPE  BIGINT,
CALLERSUBGROUP  varchar(255),
CALLEESUBGROUP  varchar(255),
ORICALLEDNUMBER  varchar(255),
ORICALLINGNUMBER  varchar(255),
CALLERPNP  varchar(255),
CALLEEPNP  varchar(255),
REROUTE  varchar(255),
ACRCALLID  varchar(255),
GROUPNUMBER  varchar(255),
CALLCATEGORY  BIGINT,
CHARGETYPE  BIGINT,
USERPIN  varchar(255),
ACRTYPE  BIGINT,
VIDEOCALLFLAG  BIGINT,
SERVICEID  varchar(255),
FORWARDNUMBER  varchar(255),
EXTFORWARDNUMBER  varchar(100),
SRFMSGID  varchar(100),
MSSERVER  varchar(255),
BEGBIGINTIME  varchar(255),
RELEASECAUSE  BIGINT,
RELEASEREASON  varchar(255),
AREANUMBER  BIGINT)
-------------------------------impala sql--------------------------------------------------------------
#400/800 接通电话电话次数
SELECT CALLINGPARTYNUMBER as fw,count(*) as n from mphone
where substr(STARTDATEANDTIME,1,8) = "20170629"
and substr(CALLINGPARTYNUMBER,1,3) = "400" or substr(CALLINGPARTYNUMBER,1,3) = "800" and CALLTYPE = 1
GROUP by fw
order by n DESC;
#手机 接通电话电话次数
SELECT CALLINGPARTYNUMBER as mobile,count(*) as n from mphone
where  substr(STARTDATEANDTIME,1,8) = "20170629"
and substring(CALLINGPARTYNUMBER,1,4) = "+861" and LENGTH(CALLINGPARTYNUMBER) = 14 and CALLTYPE = 1
GROUP by mobile 
ORDER BY n DESC;
#座机 接通电话电话次数
SELECT CALLINGPARTYNUMBER as tele,count(*) as n  from mphone
where  substr(STARTDATEANDTIME,1,8) = "20170629"
and substring(CALLINGPARTYNUMBER,1,1) = "0" and substring(CALLINGPARTYNUMBER,2,1) <> "0" and CALLTYPE = 1
GROUP by tele 
ORDER BY n DESC;
#境外 接通电话电话次数
SELECT CALLINGPARTYNUMBER as ext,count(*) as n from mphone
where substr(STARTDATEANDTIME,1,8) = "20170629"
and substring(CALLINGPARTYNUMBER,1,2) = "00" and CALLTYPE = 1
GROUP BY ext 
order by n DESC;
#其他 接通电话电话次数
SELECT CALLINGPARTYNUMBER as other,count(*) as n from mphone
where substr(STARTDATEANDTIME,1,8) = "20170629"
and CALLTYPE = 1 and LENGTH(CALLINGPARTYNUMBER) < 14 and substring(CALLINGPARTYNUMBER,1,1) <> "0"
GROUP BY other 
order by n DESC;

#平均拨打时长(日)
SELECT CALLINGPARTYNUMBER as phone,avg(DURATION) as n from mphone
where substr(STARTDATEANDTIME,1,8) = "20170629"  and  CALLINGPARTYNUMBER <> "-"
GROUP BY phone
ORDER BY n DESC
#拨打次数（日）
SELECT CALLINGPARTYNUMBER as phone, count(*) as n from mphone 
where substr(STARTDATEANDTIME,1,8) = "20170629"  and CALLINGPARTYNUMBER <> "-"
GROUP BY phone 
ORDER BY n DESC;
#振铃释放比例（日）
SELECT a.phone,(a.n/b.n) as n from (
SELECT * FROM(
SELECT CALLINGPARTYNUMBER as phone,count(*) as n from mphone  
where substr(STARTDATEANDTIME,1,8) = "20170629" and CALLTYPE = 1 and  CALLINGPARTYNUMBER <> "-"
and RELEASECAUSE = "0"
GROUP BY phone) as c
where c.n > 1) as a 
LEFT JOIN
(SELECT CALLINGPARTYNUMBER as phone,count(*) as n from mphone 
where substr(STARTDATEANDTIME,1,8) = "20170629" and CALLTYPE = 1 and  CALLINGPARTYNUMBER <> "-"
GROUP BY phone ) as b
on a.phone = b.phone
GROUP BY a.phone ,n
ORDER BY n DESC;
#平均振铃时长(日)
SELECT CALLINGPARTYNUMBER as phone,avg(VIDEOCALLFLAG) as n from mphone
where substr(STARTDATEANDTIME,1,8) = "20170629" and CALLINGPARTYNUMBER <> "-"
GROUP BY phone
ORDER BY n DESC;
#被叫号段集中度(日) explain 
SELECT f.phone,f.concentrate FROM (
SELECT c.phone ,(c.max/d.n) as concentrate from (
SELECT b.phone,max(b.nn) as max  from (
SELECT h.* FROM(
SELECT a.phone ,a.n,count(a.n) as nn from (
SELECT CALLINGPARTYNUMBER as phone ,substring(CALLEDPARTYNUMBER,1,9) as n from mphone
where substr(STARTDATEANDTIME,1,8) = "20170629" and CALLINGPARTYNUMBER <> "-"
GROUP BY CALLINGPARTYNUMBER,CALLEDPARTYNUMBER) as a 
GROUP BY a.phone,a.n) as h
where h.nn > 5) as b
GROUP BY b.phone ) as c 
LEFT JOIN
(SELECT g.phone ,count(g.n) as n  FROM(
SELECT CALLINGPARTYNUMBER as phone ,CALLEDPARTYNUMBER as n from mphone
where substr(STARTDATEANDTIME,1,8) = "20170629" and CALLINGPARTYNUMBER <> "-"
GROUP BY phone,CALLEDPARTYNUMBER)as g
GROUP BY g.phone ) as d 
on c.phone= d.phone
group by c.phone,concentrate) as f
where f.concentrate < 1
ORDER BY f.concentrate DESC;


# sparksql   http://www.infoq.com/cn/articles/apache-spark-sql
import org.apache.spark.SparkContext
# hiveContext application
val hiveContext=new org.apache.spark.sql.hive.HiveContext(sc)
import hiveContext._
hiveContext.sql("show tables").take(10) //取前十个表看看
hiveContext.sql("show tables").show()
-----------------------------------------------------------------------------------
# sqlContext application
import org.apache.spark.sql.SQLContext
val sqlContext = new org.apache.spark.sql.SQLContext(sc)  
import sqlContext._  
sqlContext.sql("show tables").show

/*
文本文件customers.txt中的内容如下：
100, John Smith, Austin, TX, 78727
200, Joe Johnson, Dallas, TX, 75201
300, Bob Jones, Houston, TX, 77028
400, Andy Davis, San Antonio, TX, 78227
500, James Williams, Austin, TX, 78727
 */
----------------------------------------------------------------------------------
// 首先用已有的Spark Context对象创建SQLContext对象
val sqlContext = new org.apache.spark.sql.SQLContext(sc)

// 导入语句，可以隐式地将RDD转化成DataFrame
import sqlContext.implicits._

// 创建一个表示客户的自定义类
case class Customer(customer_id: Int, name: String, city: String, state: String, zip_code: String)

// 用数据集文本文件创建一个Customer对象的DataFrame
val dfCustomers = sc.textFile("data/customers.txt").map(_.split(",")).map(p => Customer(p(0).trim.toInt, p(1), p(2), p(3), p(4))).toDF()

// 将DataFrame注册为一个表
dfCustomers.registerTempTable("customers")

// 显示DataFrame的内容
dfCustomers.show()

// 打印DF模式
dfCustomers.printSchema()

// 选择客户名称列
dfCustomers.select("name").show()

// 选择客户名称和城市列
dfCustomers.select("name", "city").show()

// 根据id选择客户
dfCustomers.filter(dfCustomers("customer_id").equalTo(500)).show()

// 根据邮政编码统计客户数量
dfCustomers.groupBy("zip_code").count().show()
//在上一示例中，模式是通过反射而得来的。我们也可以通过编程的方式指定数据集的模式。
//这种方法在由于数据的结构以字符串的形式编码而无法提前定义定制类的情况下非常实用。
//如下代码示例展示了如何使用新的数据类型类StructType，StringType和StructField指定模式。

//
// 用编程的方式指定模式
//

// 用已有的Spark Context对象创建SQLContext对象
val sqlContext = new org.apache.spark.sql.SQLContext(sc)

// 创建RDD对象
val rddCustomers = sc.textFile("data/customers.txt")

// 用字符串编码模式
val schemaString = "customer_id name city state zip_code"

// 导入Spark SQL数据类型和Row
import org.apache.spark.sql._

import org.apache.spark.sql.types._;

// 用模式字符串生成模式对象
val schema = StructType(schemaString.split(" ").map(fieldName => StructField(fieldName, StringType, true)))

// 将RDD（rddCustomers）记录转化成Row。
val rowRDD = rddCustomers.map(_.split(",")).map(p => Row(p(0).trim,p(1),p(2),p(3),p(4)))

// 将模式应用于RDD对象。
val dfCustomers = sqlContext.createDataFrame(rowRDD, schema)

// 将DataFrame注册为表
dfCustomers.registerTempTable("customers")

// 用sqlContext对象提供的sql方法执行SQL语句。
val custNames = sqlContext.sql("SELECT name FROM customers")

// SQL查询的返回结果为DataFrame对象，支持所有通用的RDD操作。
// 可以按照顺序访问结果行的各个列。
custNames.map(t => "Name: " + t(0)).collect().foreach(println)

// 用sqlContext对象提供的sql方法执行SQL语句。
val customersByCity = sqlContext.sql("SELECT name,zip_code FROM customers ORDER BY zip_code")

// SQL查询的返回结果为DataFrame对象，支持所有通用的RDD操作。
// 可以按照顺序访问结果行的各个列。
customersByCity.map(t => t(0) + "," + t(1)).collect().foreach(println)

scala> sqlContext.sql("show tables").show 
scala> sqlContext.sql("select * from predict limit 10").show  

























#400/800 接通电话电话次数
SELECT CALLINGPARTYNUMBER as "400/800",count(*) as n from phone.all 
where date_format(STARTDATEANDTIME, '%Y-%m-%d') = "2017-06-29"
and substring(CALLINGPARTYNUMBER,1,3) = "400" or substring(CALLINGPARTYNUMBER,1,3) = "800" and CALLTYPE = "1"
GROUP by "400/800"  order by n DESC;
#手机 接通电话电话次数
SELECT CALLINGPARTYNUMBER as mobile,count(*) as n from phone.all 
where date_format(STARTDATEANDTIME, '%Y-%m-%d') = "2017-06-29"
and substring(CALLINGPARTYNUMBER,1,4) = "+861" and LENGTH(CALLINGPARTYNUMBER) = 14 and CALLTYPE = "1"
GROUP by mobile ORDER BY n DESC;
#座机 接通电话电话次数
SELECT CALLINGPARTYNUMBER as tele,count(*) as n  from phone.all 
where  date_format(STARTDATEANDTIME, '%Y-%m-%d') = "2017-06-29"
and substring(CALLINGPARTYNUMBER,1,1) = "0" and substring(CALLINGPARTYNUMBER,2,1) <> "0" and CALLTYPE = "1"
GROUP by tele ORDER BY n DESC;
#境外 接通电话电话次数
SELECT CALLINGPARTYNUMBER as external ,count(*) as n from phone.all 
where date_format(STARTDATEANDTIME, '%Y-%m-%d') = "2017-06-29"
and substring(CALLINGPARTYNUMBER,1,2) = "00" and CALLTYPE = "1"
GROUP BY external order by n DESC;
#其他 接通电话电话次数
SELECT CALLINGPARTYNUMBER as other,count(*) as n from phone.all 
where date_format(STARTDATEANDTIME, '%Y-%m-%d') = "2017-06-29"
and CALLTYPE = "1" and LENGTH(CALLINGPARTYNUMBER) < 14 and substring(CALLINGPARTYNUMBER,1,1) <> "0"
GROUP BY other order by n DESC;

#平均拨打时长(日)
SELECT CALLINGPARTYNUMBER as phone,avg(DURATION) as n from phone.all
where date_format(STARTDATEANDTIME, '%Y-%m-%d') = "2017-06-29" and CALLINGPARTYNUMBER <> "-"
GROUP BY phone ORDER BY n DESC
#拨打次数（日）
SELECT CALLINGPARTYNUMBER as phone, count(*) as n from phone.all 
where date_format(STARTDATEANDTIME, '%Y-%m-%d') = "2017-06-29" and CALLINGPARTYNUMBER <> "-"
GROUP BY phone ORDER BY n DESC;
#振铃释放比例（日）
SELECT a.phone,(a.n/b.n) as n from (
SELECT * FROM(
SELECT CALLINGPARTYNUMBER as phone,count(*) as n from phone.all 
where date_format(STARTDATEANDTIME, '%Y-%m-%d') = "2017-06-29" and CALLTYPE = 1 and  CALLINGPARTYNUMBER <> "-"
and RELEASECAUSE = 0
GROUP BY phone) as c
where c.n > 1) as a 
LEFT JOIN
(SELECT CALLINGPARTYNUMBER as phone,count(*) as n from phone.all 
where date_format(STARTDATEANDTIME, '%Y-%m-%d') = "2017-06-29" and CALLTYPE = 1 and  CALLINGPARTYNUMBER <> "-"
GROUP BY phone ) as b
on a.phone = b.phone
GROUP BY a.phone,n 
ORDER BY n DESC;
#平均振铃时长(日)
SELECT CALLINGPARTYNUMBER as phone,avg(VIDEOCALLFLAG) as n from phone.all 
where date_format(STARTDATEANDTIME, '%Y-%m-%d') = "2017-06-29" and CALLINGPARTYNUMBER <> "-"
GROUP BY phone ORDER BY n DESC;
#被叫号段集中度(日) explain 
SELECT f.phone,f.concentrate FROM (
SELECT c.phone ,(c.max/d.n) as concentrate from (
SELECT b.phone,max(b.nn) as max from (
SELECT h.* FROM(SELECT a.phone ,a.n,count(a.n) as nn from (
SELECT CALLINGPARTYNUMBER as phone ,substring(CALLEDPARTYNUMBER,1,9) as n from phone.all
where date_format(STARTDATEANDTIME, '%Y-%m-%d') = "2017-06-29" and CALLINGPARTYNUMBER <> "-"
) as a 
GROUP BY a.phone,a.n) as h
where h.nn  > 5) as b
GROUP BY b.phone ) as c 
LEFT JOIN
(SELECT g.phone ,count(g.n) as n  FROM(
SELECT CALLINGPARTYNUMBER as phone ,CALLEDPARTYNUMBER as n from phone.all
where date_format(STARTDATEANDTIME, '%Y-%m-%d') = "2017-06-29" and CALLINGPARTYNUMBER <> "-"
GROUP BY phone,n)as g
GROUP BY g.phone) as d 
on c.phone= d.phone
group by c.phone,concentrate) as f
where f.concentrate < 1
ORDER BY f.concentrate DESC;



update phone.all set CALLINGPARTYNUMBER = "-" where CALLINGPARTYNUMBER is NULL;









































