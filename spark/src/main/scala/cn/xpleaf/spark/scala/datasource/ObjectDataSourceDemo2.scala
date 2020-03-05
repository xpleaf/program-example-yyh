package cn.xpleaf.spark.scala.datasource

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.sources.{BaseRelation, SchemaRelationProvider, TableScan}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Row, SQLContext, SparkSession}

class PersonSchemaRelationProvider extends SchemaRelationProvider {
  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String], schema: StructType): BaseRelation = {
    PersonSchemaRelation(sqlContext.sparkSession, schema)
  }
}

case class PersonSchemaRelation(sparkSession: SparkSession, structType: StructType) extends BaseRelation with TableScan {

  val localData: Seq[Seq[Any]] = List(
    List("xpleaf", 26, 1500.00),
    List("yyh", 26, 1600.00)
  )

  override def sqlContext: SQLContext = sparkSession.sqlContext

  override def schema: StructType = structType

  override def buildScan(): RDD[Row] = sparkSession.sparkContext.parallelize(localData).map(row => {
    val name = row.head
    val age = row(1)
    val salary = row(2)
    Row(name, age, salary)
  })
}

object ObjectDataSourceDemo2 {

  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder()
      .master("local[2]")
      .getOrCreate()

    sparkSession.sql("create table person(name string,age int,salary double) " +
      "using cn.xpleaf.spark.scala.datasource.PersonSchemaRelationProvider").show()

    sparkSession.sql("select * from person").show()

    sparkSession.stop()
  }

}