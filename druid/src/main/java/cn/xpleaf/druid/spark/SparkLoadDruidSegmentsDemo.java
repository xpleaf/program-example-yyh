package cn.xpleaf.druid.spark;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.commons.io.IOUtils;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.indexer.HadoopDruidIndexerConfig;
import org.apache.druid.indexer.hadoop.DatasourceIngestionSpec;
import org.apache.druid.indexer.hadoop.DatasourceInputFormat;
import org.apache.druid.indexer.hadoop.WindowedDataSegment;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.query.filter.DimFilter;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.server.security.AuthenticationResult;
import org.apache.druid.sql.calcite.expression.Expressions;
import org.apache.druid.sql.calcite.planner.PlannerConfig;
import org.apache.druid.sql.calcite.planner.PlannerContext;
import org.apache.druid.sql.calcite.rel.VirtualColumnRegistry;
import org.apache.druid.sql.calcite.table.RowSignature;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.TimelineObjectHolder;
import org.apache.druid.timeline.VersionedIntervalTimeline;
import org.apache.druid.timeline.partition.PartitionChunk;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.*;
import org.joda.time.Interval;
import org.spark_project.guava.collect.Ordering;
import scala.Tuple2;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * begin to do this, you must set up a druid server and hadoop server, and set the druid deep storage to hdfs.
 * Reference:
 * https://druid.apache.org/docs/latest/development/extensions-core/hdfs.html#configuration-for-hdfs
 *
 * 1.Set up the druid conf
 * /Users/yyh/app/apache-druid-0.17.0/conf/druid/single-server/micro-quickstart/_common
 *  druid.storage.type=hdfs
 *  druid.storage.storageDirectory=/druid/segments
 *
 * 2.Copy the hadoop conf the druid _common conf directory
 * cp app/hadoop-2.7.7/etc/hadoop/* /Users/yyh/app/apache-druid-0.17.0/conf/druid/single-server/micro-quickstart/_common
 *
 * 3.Start micro server
 * start-micro-quickstart
 *
 * after then, you can reload the data from local disk,
 * and you will find druid load the segment into the deep storage of hfds instead of local disk
 */
public class SparkLoadDruidSegmentsDemo {

    // DATASOURCE
    private static final String DATASOURCE = "wikipedia_hdfs";

    // use for segment filter
    private static final Interval INTERVAL = new Interval("2015-09-12T00:00:00.000Z/2015-09-13T00:00:00.000Z");
    private static final List<Interval> INTERVALS = ImmutableList.of(INTERVAL);

    // use for building object associated with calcite
    private static final JavaTypeFactoryImpl TYPE_FACTORY = new JavaTypeFactoryImpl();
    private static final RexBuilder REX_BUILDER = new RexBuilder(TYPE_FACTORY);

    // use for jobConf
    private static final String CONF_DATASOURCES = "druid.datasource.input.datasources";
    private static final String CONF_SCHEMA = "druid.datasource.input.schema";
    private static final String CONF_SEGMENTS = "druid.datasource.input.segments";
    private static final String CONF_MAX_SPLIT_SIZE = "druid.datasource.input.split.max.size";

    public static void main(String[] args) throws IOException {
        // in windows, may be you need to set up the hadoop home and winutils
        JobConf jobConf = createJobConf();
        System.out.println(jobConf);

        SparkSession sparkSession = SparkSession.builder().master("local[2]").getOrCreate();
        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkSession.sparkContext());

        JavaPairRDD<NullWritable, InputRow> rdd = javaSparkContext.newAPIHadoopRDD(jobConf,
                DatasourceInputFormat.class, NullWritable.class, InputRow.class);
        rdd.cache();

        // rdd before filter
        System.out.println("Before filter: " + rdd.count());

        // rdd after filter
        JavaPairRDD<NullWritable, InputRow> filteredRDD = rdd.filter(tuple2 ->
                Optional.ofNullable(tuple2._2().getRaw("channel"))
                        .filter(channel -> "#en.wikipedia".equals(channel)
                                || "#es.wikipedia".equals(channel) || "#ca.wikipedia".equals(channel))
                        .isPresent());
        filteredRDD.cache();
        System.out.println("After filter: " + filteredRDD.count());

        // group by and print the gbk res
        JavaPairRDD<Object, Number> pairRDD = filteredRDD.mapToPair(tuple2 -> {
            Object channel = tuple2._2.getRaw("channel");   // we only select dimension channel
            Number added = tuple2._2.getMetric("added");    // we only select metrics added
            return new Tuple2<>(channel, added);
        });
        pairRDD.groupByKey().foreach(tuple2 ->
                System.out.println(StringUtils
                        .format("key: %s, value: %s", tuple2._1, "to many, ignore")));
    }

    // create jobConf for reading data from hdfs
    private static JobConf createJobConf() throws IOException {
        // download segment info and deserialization
        String segmentsUrl = "http://localhost:8081/druid/coordinator/v1/metadata/datasources/" + DATASOURCE;
        HttpGet request = new HttpGet(segmentsUrl);
        request.setHeader("Content-Type", "application/json");
        CloseableHttpClient httpClient = HttpClientBuilder.create().build();
        CloseableHttpResponse response = httpClient.execute(request);
        String res = IOUtils.toString(response.getEntity().getContent(), StandardCharsets.UTF_8);
        String segmentsList = HadoopDruidIndexerConfig.JSON_MAPPER.readValue(res, JsonNode.class).get("segments").toString();
        List<DataSegment> dataSegments = HadoopDruidIndexerConfig.JSON_MAPPER.readValue(segmentsList,
                new TypeReference<List<DataSegment>>() {});

        // prepare for building jobConf

        // filter
        DimFilter filter = getFilter();

        // dimensions(the dimension column you want to read from the datasource)
        ImmutableList<String> dimensions = ImmutableList.of("channel");

        // metrics(the metrics column you want to read from the datasource)
        ImmutableList<String> metrics = ImmutableList.of("added");

        // ingestionSpec
        DatasourceIngestionSpec spec = new DatasourceIngestionSpec(DATASOURCE, null,
                INTERVALS, dataSegments, filter, dimensions, metrics, false, null);

        List<WindowedDataSegment> segments = generateInputSegments(dataSegments);

        // begin to build jobConf
        JobConf jobConf = new JobConf();
        // 下面的可以参考druid源码的设置
        jobConf.set(CONF_DATASOURCES,
                HadoopDruidIndexerConfig.JSON_MAPPER.writeValueAsString(ImmutableList.of(DATASOURCE)));
        jobConf.set(StringUtils.format("%s.%s", CONF_SCHEMA, spec.getDataSource()),
                HadoopDruidIndexerConfig.JSON_MAPPER.writeValueAsString(spec));
        jobConf.set(StringUtils.format("%s.%s", CONF_SEGMENTS, spec.getDataSource()),
                HadoopDruidIndexerConfig.JSON_MAPPER.writeValueAsString(segments));

        return jobConf;
    }

    // get the required segment for a specific time range
    private static List<WindowedDataSegment> generateInputSegments(List<DataSegment> segments) {
        VersionedIntervalTimeline<String, DataSegment> timeline = new VersionedIntervalTimeline<>(Ordering.natural());
        for (DataSegment segment : segments) {
            timeline.add(segment.getInterval(), segment.getVersion(), segment.getShardSpec().createChunk(segment));
        }
        List<WindowedDataSegment> windowedDataSegments = new ArrayList<>();
        // timeline中包含的是所有segment分片信息的对象
        // 这里的intervals是指期待获取的数据的范围，所以就要从timeline中找在这个intervals范围中的segment分片
        // 然后将其包含部分添加到windowedSegments中，其命名也比较好地代表了其含义
        for (Interval interval : INTERVALS) {
            List<TimelineObjectHolder<String, DataSegment>> timeLineSegments = timeline.lookup(interval);
            for (TimelineObjectHolder<String, DataSegment> holder : timeLineSegments) {
                for (PartitionChunk<DataSegment> chunk : holder.getObject()) {
                    windowedDataSegments.add(new WindowedDataSegment(chunk.getObject(), holder.getInterval()));
                }
            }
        }
        return windowedDataSegments;
    }

    // get filter used by druid DatasourceIngestionSpec
    private static DimFilter getFilter() {
        AuthenticationResult authenticationResult =
                new AuthenticationResult("test", "test", "test", ImmutableMap.of());
        PlannerContext plannerContext = PlannerContext.create(null, null, new PlannerConfig(),
                ImmutableMap.of(), authenticationResult);
        RexNode rexNode = REX_BUILDER.makeCall(
                TYPE_FACTORY.createSqlType(SqlTypeName.BIGINT),
                SqlStdOperatorTable.LESS_THAN,
                ImmutableList.of(inputRef("__time"),
                        REX_BUILDER.makeBigintLiteral(BigDecimal.valueOf(1583128800000L)))
        );
        return Expressions.toFilter(plannerContext, getRowSignature(),
                VirtualColumnRegistry.create(getRowSignature()), rexNode);
    }

    // build rexNode
    private static RexNode inputRef(String columnName) {
        int columnNumber = getRowSignature().getRowOrder().indexOf(columnName);
        return REX_BUILDER.makeInputRef(
                getRowSignature().getRelDataType(TYPE_FACTORY).getFieldList().get(columnNumber).getType(),
                columnNumber);
    }

    // get spark sql schema
    private static StructType getSchema() {
        StructType schema = new StructType(new StructField[]{
                DataTypes.createStructField("__time", DataTypes.LongType, true),
                DataTypes.createStructField("added", DataTypes.LongType, true),
                DataTypes.createStructField("channel", DataTypes.StringType, true),
                DataTypes.createStructField("cityName", DataTypes.StringType, true),
                DataTypes.createStructField("comment", DataTypes.StringType, true),
                DataTypes.createStructField("countryIsoCode", DataTypes.StringType, true),
                DataTypes.createStructField("countryName", DataTypes.StringType, true),
                DataTypes.createStructField("deleted", DataTypes.LongType, true),
                DataTypes.createStructField("delta", DataTypes.LongType, true),
                DataTypes.createStructField("isAnonymous", DataTypes.StringType, true),
                DataTypes.createStructField("isMinor", DataTypes.StringType, true),
                DataTypes.createStructField("isNew", DataTypes.StringType, true),
                DataTypes.createStructField("isRobot", DataTypes.StringType, true),
                DataTypes.createStructField("isUnpatrolled", DataTypes.StringType, true),
                DataTypes.createStructField("metroCode", DataTypes.LongType, true),
                DataTypes.createStructField("namespace", DataTypes.StringType, true),
                DataTypes.createStructField("page", DataTypes.StringType, true),
                DataTypes.createStructField("regionIsoCode", DataTypes.StringType, true),
                DataTypes.createStructField("regionName", DataTypes.StringType, true),
                DataTypes.createStructField("user", DataTypes.StringType, true),
        });
        return schema;
    }

    // RowSignature used by druid sql, translating druid columnType to calcite sqlType
    // this method map spark sql dataType to druid RowSignature
    private static RowSignature getRowSignature() {
        RowSignature.Builder builder = RowSignature.builder();
        for (StructField structField : getSchema().fields()) {
            builder.add(structField.name(), mapCalciteValueType(structField.dataType()));
        }
        return builder.build();
    }

    // map spark sql dataType to druid columnType
    private static ValueType mapCalciteValueType(DataType dataType) {
        if (dataType instanceof StringType) {
            return ValueType.STRING;
        } else if (dataType instanceof LongType) {
            return ValueType.LONG;
        } else if (dataType instanceof DoubleType) {
            return ValueType.DOUBLE;
        }
        return ValueType.COMPLEX;
    }

}
