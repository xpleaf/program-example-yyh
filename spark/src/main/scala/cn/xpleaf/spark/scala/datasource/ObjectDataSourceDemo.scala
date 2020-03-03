package cn.xpleaf.spark.scala.datasource

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext, SparkSession}
import org.apache.spark.sql.sources.{BaseRelation, RelationProvider, TableScan}
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}

class PersonRelationProvider extends RelationProvider {
  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String]): BaseRelation = {
    PersonRelation(sqlContext.sparkSession)
  }
}

case class PersonRelation(sparkSession: SparkSession) extends BaseRelation with TableScan {

  val localData: Seq[Seq[Any]] = List(
    List("xpleaf", 26, 1500.00),
    List("yyh", 26, 1600.00)
  )

  override def sqlContext: SQLContext = sparkSession.sqlContext

  override def schema: StructType = StructType(List(
    StructField("name", DataTypes.StringType),
    StructField("age", DataTypes.IntegerType),
    StructField("salary", DataTypes.DoubleType)
  ))

  override def buildScan(): RDD[Row] = sparkSession.sparkContext.parallelize(localData).map(row => {
    val name = row.head
    val age = row(1)
    val salary = row(2)
    Row(name, age, salary)
  })
}

object ObjectDataSourceDemo {

  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder()
      .master("local[2]")
      .getOrCreate()

    val df = sparkSession.read
      .format("cn.xpleaf.spark.scala.datasource.PersonRelationProvider")
      .option("not_use_key", "not_use_value")
      .load()
    df.show()
    
    df.createOrReplaceTempView("person")
    sparkSession.sql("select count(*) as total from person").show()

    sparkSession.stop()
  }

}
