package cn.xpleaf.druid.spark.datasource;

import cn.xpleaf.druid.spark.job.LoadDruidSegmentsToRddJob;
import org.apache.druid.data.input.InputRow;
import org.apache.hadoop.io.NullWritable;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.sources.BaseRelation;
import org.apache.spark.sql.sources.TableScan;
import org.apache.spark.sql.types.StructType;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

public class DruidRelation extends BaseRelation implements TableScan {

    private final SparkSession sparkSession;
    private final StructType schema;

    public DruidRelation(SparkSession sparkSession, StructType schema) {
        this.sparkSession = sparkSession;
        this.schema = schema;
    }

    @Override
    public SQLContext sqlContext() {
        return sparkSession.sqlContext();
    }

    @Override
    public StructType schema() {
        return schema;
    }

    @Override
    public RDD<Row> buildScan() {
        // you should build a job to scan the data every time this method invoked
        LoadDruidSegmentsToRddJob segmentsToRddJob = new LoadDruidSegmentsToRddJob(schema, "wikipedia_hdfs");
        JavaPairRDD<NullWritable, InputRow> rdd = segmentsToRddJob.loadDruidSegmentsToRdd(sparkSession);

        // note: 下面这些变量，不要在rdd算子操作中直接使用segmentsToRddJob和schema
        // 不然就会报Task not serializable异常，因为这些task是要发送到各executor上去的
        // 即使segmentsToRddJob实现序列化没有问题，因为 是我们定义的类，但是schema是StructType的，所以就尽量不要这样操作
        List<String> dimensions = segmentsToRddJob.getDimensions();
        List<String> metrics = segmentsToRddJob.getMetrics();
        String[] fieldNames = schema.fieldNames();

        // note: the row value's order must map to the schema
        JavaRDD<Row> rowJavaRDD = rdd.map(tuple2 -> {
            InputRow inputRow = tuple2._2;
            LinkedList<Object> rowData = new LinkedList<>();
            Arrays.stream(fieldNames).forEach(fieldName -> {
                if (dimensions.contains(fieldName)) {
                    rowData.add(inputRow.getRaw(fieldName));
                } else if (metrics.contains(fieldName)) {
                    // in druid, only __time map to this method's return value
                    if ("__time".equals(fieldName)) {
                        rowData.add(inputRow.getTimestampFromEpoch());
                    } else {
                        rowData.add(inputRow.getMetric(fieldName));
                    }
                }
            });
            // note: must pass an array
            return RowFactory.create(rowData.toArray());
        });

        // JavaRDD wraps the scala rdd, so return the wrapped rdd
        return rowJavaRDD.rdd();
    }
}
