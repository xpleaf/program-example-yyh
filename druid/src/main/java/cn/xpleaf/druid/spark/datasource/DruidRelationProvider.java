package cn.xpleaf.druid.spark.datasource;

import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.sources.BaseRelation;
import org.apache.spark.sql.sources.SchemaRelationProvider;
import org.apache.spark.sql.types.StructType;
import scala.collection.immutable.Map;

public class DruidRelationProvider implements SchemaRelationProvider {
    @Override
    public BaseRelation createRelation(SQLContext sqlContext, Map<String, String> parameters, StructType schema) {
        return new DruidRelation(sqlContext.sparkSession(), schema);
    }
}
