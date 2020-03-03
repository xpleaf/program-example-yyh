package cn.xpleaf.spark.scala.datasource

import org.apache.spark.sql.SparkSession

object CsvDataSourceDemo {

  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder()
      .master("local[2]")
      .getOrCreate()

    val df = sparkSession.read.format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load("spark/src/main/resources/person.csv")
    df.show()

    df.createOrReplaceTempView("person")
    sparkSession.sql("select count(*) from person").show()

    sparkSession.stop()
  }

}
