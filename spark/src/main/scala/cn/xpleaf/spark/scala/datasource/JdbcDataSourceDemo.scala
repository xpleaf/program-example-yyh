package cn.xpleaf.spark.scala.datasource

import org.apache.spark.sql.SparkSession

object JdbcDataSourceDemo {

  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder()
      .master("local[2]")
      .getOrCreate()

    val df = sparkSession.read
      .format("jdbc")
      .option("driver", "com.mysql.jdbc.Driver")
      .option("url", "jdbc:mysql://localhost:3306")
      .option("dbtable", "school.teacher")
      .option("user", "root")
      .option("password", "root123456")
      .load()
    df.show()

    df.createOrReplaceTempView("person")
    sparkSession.sql("select count(*) as total from person").show()

    sparkSession.stop()
  }

}

/*
1.
In spark beeline, if you want to create a table map to the jdbc datasource, you can create table like this:

create table teacher(name string,age int,salary double)
using jdbc
options(driver='com.mysql.jdbc.Driver',url='jdbc:mysql://localhost:3306',dbtable='school.teacher',user='root',password='root123456');

---------------------------------------------------------------------------------------------------------------------------------

2.
After then, you can query the table with spark sql which will help you scan the data from the jdbc datasource:

2: jdbc:hive2://localhost:10000> select * from teacher;
+---------+------+----------+--+
|  name   | age  |  salary  |
+---------+------+----------+--+
| xpleaf  | 26   | 1500.0  |
+---------+------+----------+--+
1 row selected (35.262 seconds)

---------------------------------------------------------------------------------------------------------------------------------

3.
Also, you can write the data to hdfs using csv format with spark sql:

insert overwrite directory '/tmp/tables/teacher' using csv select * from teacher;

and you will find the data in the hdfs with csv format:

yeyonghaodeMacBook-Pro:~ yyh$ hdfs dfs -ls /tmp/tables/teacher
Found 2 items
-rw-r--r--   1 anonymous supergroup          0 2020-03-04 00:25 /tmp/tables/teacher/_SUCCESS
-rw-r--r--   1 yyh       supergroup         18 2020-03-04 00:25 /tmp/tables/teacher/part-00000-6cc7f2e7-0c3f-4f9f-9963-e692559ee188-c000.csv
yeyonghaodeMacBook-Pro:~ yyh$ hdfs dfs -cat /tmp/tables/teacher/part-00000-6cc7f2e7-0c3f-4f9f-9963-e692559ee188-c000.csv
xpleaf,26,1500.0

 */