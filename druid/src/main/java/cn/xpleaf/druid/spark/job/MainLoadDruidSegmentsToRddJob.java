package cn.xpleaf.druid.spark.job;

import org.apache.druid.data.input.InputRow;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;

public class MainLoadDruidSegmentsToRddJob {

    public static void main(String[] args) {
        // only dimension channel and metrics __time/added
        StructType schema = new StructType(new StructField[]{
                DataTypes.createStructField("__time", DataTypes.LongType, true),
                DataTypes.createStructField("added", DataTypes.LongType, true),
                DataTypes.createStructField("channel", DataTypes.StringType, true),
        });
        String datasource = "wikipedia_hdfs";
        LoadDruidSegmentsToRddJob loadDruidSegmentsToRddJob = new LoadDruidSegmentsToRddJob(schema, datasource);
        SparkSession sparkSession = SparkSession.builder().master("local[2]").getOrCreate();

        JavaPairRDD<NullWritable, InputRow> rdd = loadDruidSegmentsToRddJob.loadDruidSegmentsToRdd(sparkSession);
        System.out.println(rdd.count());

        // group by and print the gbk res
        JavaPairRDD<Object, Number> pairRDD = rdd.mapToPair(tuple2 -> {
            Object channel = tuple2._2.getRaw("channel");   // we only select dimension channel
            Number added = tuple2._2.getMetric("added");    // we only select metrics added
            return new Tuple2<>(channel, added);
        });
        pairRDD.groupByKey().foreach(tuple2 ->
                System.out.println(StringUtils
                        .format("key: %s, value: %s", tuple2._1, "too many, ignore")));

        sparkSession.stop();
    }

}
