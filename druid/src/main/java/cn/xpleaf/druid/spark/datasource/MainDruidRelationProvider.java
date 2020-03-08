package cn.xpleaf.druid.spark.datasource;

import org.apache.spark.sql.SparkSession;

public class MainDruidRelationProvider {

    public static void main(String[] args) {
        SparkSession sparkSession = SparkSession.builder().master("local[2]").getOrCreate();

        sparkSession.sql("create table wiki(__time long,added long,channel string) " +
                "using cn.xpleaf.druid.spark.datasource.DruidRelationProvider").show();

        sparkSession.sql("select channel,count(*) as total from wiki " +
                "group by channel order by total desc limit 3").show();

        sparkSession.stop();
    }

}
