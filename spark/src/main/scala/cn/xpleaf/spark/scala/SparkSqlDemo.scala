package cn.xpleaf.spark.scala

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

/**
 * Reference:
 * https://blog.51cto.com/xpleaf/2114298
 */
object SparkSqlDemo {

  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder()
      .master("local[2]")
      .getOrCreate()

    val rowRDD: RDD[Row] = sparkSession.sparkContext.parallelize(Array(List("xpleaf", 26, 1500.00))).map(rowList => {
      val name = rowList.head
      val age = rowList(1)
      val salary = rowList(2)
      Row(name, age, salary)
    })

    val schema: StructType = StructType(List(
      StructField("name", DataTypes.StringType),
      StructField("age", DataTypes.IntegerType),
      StructField("salary", DataTypes.DoubleType)
    ))

    val df: DataFrame = sparkSession.createDataFrame(rowRDD, schema)
    df.show()

    df.createOrReplaceTempView("person")
    sparkSession.sql("select count(*) from person").show()

    sparkSession.stop()
  }

}
