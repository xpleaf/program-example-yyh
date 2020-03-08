package cn.xpleaf.druid.spark.job;

import com.fasterxml.jackson.core.JsonProcessingException;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.spark_project.guava.collect.Ordering;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.util.*;

public class LoadDruidSegmentsToRddJob {

    private static final Logger LOGGER = LoggerFactory.getLogger(LoadDruidSegmentsToRddJob.class);

    private static final RexBuilder REX_BUILDER = new RexBuilder(new JavaTypeFactoryImpl());

    // default interval to filter the segments
    private static final Interval DEFAULT_INTERVAL = new Interval("0/3000");

    private static final String BASE_SEGMENTS_URL = "http://localhost:8081/druid/coordinator/v1/metadata/datasources/";

    // use for jobConf
    private static final String CONF_DATASOURCES = "druid.datasource.input.datasources";

    private static final String CONF_SCHEMA = "druid.datasource.input.schema";

    private static final String CONF_SEGMENTS = "druid.datasource.input.segments";

    private final StructType schema;

    private final String datasource;

    private final List<Interval> intervals;

    private RowSignature rowSignature;

    private List<String> dimensions = new LinkedList<>();

    private List<String> metrics = new LinkedList<>();

    public LoadDruidSegmentsToRddJob(StructType schema, String datasource, List<Interval> intervals) {
        Objects.requireNonNull(schema, "schema can not be null!");
        Objects.requireNonNull(datasource, "datasource can not be null!");
        this.schema = schema;
        this.datasource = datasource;
        this.intervals = intervals;
        initFromSchema();
    }

    public LoadDruidSegmentsToRddJob(StructType schema, String datasource) {
        this(schema, datasource, ImmutableList.of(DEFAULT_INTERVAL));
    }

    private void initFromSchema() {
        // init rowSignature metrics and dimensions
        RowSignature.Builder builder = RowSignature.builder();
        Arrays.stream(schema.fields()).forEach(structField -> {
            String name = structField.name();
            ValueType valueType = mapCalciteValueType(structField.dataType());
            builder.add(name, valueType);
            if (ValueType.isNumeric(valueType)) {   // default to all metrics
                metrics.add(name);
            } else {    // default to all dimensions
                dimensions.add(name);
            }
        });
        this.rowSignature = builder.build();
    }

    private RowSignature getRowSignature() {
        RowSignature.Builder builder = RowSignature.builder();
        for (StructField structField : schema.fields()) {
            builder.add(structField.name(), mapCalciteValueType(structField.dataType()));
        }
        return builder.build();
    }

    // map spark sql dataType to druid columnType
    private ValueType mapCalciteValueType(DataType dataType) {
        if (dataType instanceof StringType) {
            return ValueType.STRING;
        } else if (dataType instanceof LongType) {
            return ValueType.LONG;
        } else if (dataType instanceof DoubleType) {
            return ValueType.DOUBLE;
        }
        return ValueType.COMPLEX;
    }

    public JavaPairRDD<NullWritable, InputRow> loadDruidSegmentsToRdd(SparkSession sparkSession) {
        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkSession.sparkContext());
        JobConf jobConf = createJobConf();
        // use DatasourceInputFormat to load druid segment data to RDD
        return javaSparkContext.newAPIHadoopRDD(jobConf,
                DatasourceInputFormat.class, NullWritable.class, InputRow.class);
    }

    private JobConf createJobConf() {
        // segments info
        List<DataSegment> dataSegments = downloadSegmentsInfo();
        // filter, default to __time > 0
        DimFilter filter = getFilter();
        // ingestionSpec
        DatasourceIngestionSpec spec = new DatasourceIngestionSpec(datasource, null,
                intervals, dataSegments, filter, dimensions, metrics, false, null);
        // windowedDataSegment
        List<WindowedDataSegment> segments = generateInputSegments(dataSegments);

        // begin to build jobConf
        JobConf jobConf = new JobConf();
        // 下面的可以参考druid源码的设置
        try {
            jobConf.set(CONF_DATASOURCES,
                    HadoopDruidIndexerConfig.JSON_MAPPER.writeValueAsString(ImmutableList.of(datasource)));
            jobConf.set(StringUtils.format("%s.%s", CONF_SCHEMA, spec.getDataSource()),
                    HadoopDruidIndexerConfig.JSON_MAPPER.writeValueAsString(spec));
            jobConf.set(StringUtils.format("%s.%s", CONF_SEGMENTS, spec.getDataSource()),
                    HadoopDruidIndexerConfig.JSON_MAPPER.writeValueAsString(segments));
        } catch (JsonProcessingException e) {
            LOGGER.error("failed to parse json value, reason: {}", e.getMessage());
        }
        return jobConf;
    }

    private List<WindowedDataSegment> generateInputSegments(List<DataSegment> segments) {
        VersionedIntervalTimeline<String, DataSegment> timeline = new VersionedIntervalTimeline<>(Ordering.natural());
        for (DataSegment segment : segments) {
            timeline.add(segment.getInterval(), segment.getVersion(), segment.getShardSpec().createChunk(segment));
        }
        List<WindowedDataSegment> windowedDataSegments = new ArrayList<>();
        // timeline中包含的是所有segment分片信息的对象
        // 这里的intervals是指期待获取的数据的范围，所以就要从timeline中找在这个intervals范围中的segment分片
        // 然后将其包含部分添加到windowedSegments中，其命名也比较好地代表了其含义
        for (Interval interval : intervals) {
            List<TimelineObjectHolder<String, DataSegment>> timeLineSegments = timeline.lookup(interval);
            for (TimelineObjectHolder<String, DataSegment> holder : timeLineSegments) {
                for (PartitionChunk<DataSegment> chunk : holder.getObject()) {
                    windowedDataSegments.add(new WindowedDataSegment(chunk.getObject(), holder.getInterval()));
                }
            }
        }
        return windowedDataSegments;
    }

    private DimFilter getFilter() {
        AuthenticationResult authenticationResult =
                new AuthenticationResult("test", "test", "test", ImmutableMap.of());
        PlannerContext plannerContext = PlannerContext.create(null, null, new PlannerConfig(),
                ImmutableMap.of(), authenticationResult);
        // default to __time > 0
        RexNode rexNode = REX_BUILDER.makeCall(REX_BUILDER.getTypeFactory().createSqlType(SqlTypeName.BIGINT),
                SqlStdOperatorTable.GREATER_THAN,
                ImmutableList.of(inputRef("__time"),
                        REX_BUILDER.makeBigintLiteral(BigDecimal.valueOf(0L)))
        );
        return Expressions.toFilter(plannerContext, rowSignature, VirtualColumnRegistry.create(rowSignature), rexNode);
    }

    private RexNode inputRef(String columnName) {
        int columnNumber = rowSignature.getRowOrder().indexOf(columnName);
        return REX_BUILDER.makeInputRef(
                rowSignature.getRelDataType(REX_BUILDER.getTypeFactory()).getFieldList().get(columnNumber).getType(),
                columnNumber);
    }

    private List<DataSegment> downloadSegmentsInfo() {
        HttpGet request = new HttpGet((BASE_SEGMENTS_URL + datasource));
        request.setHeader("Content-Type", "application/json");
        try (CloseableHttpClient httpClient = HttpClientBuilder.create().build();
             CloseableHttpResponse response = httpClient.execute(request)) {
            String res = IOUtils.toString(response.getEntity().getContent(), StandardCharsets.UTF_8);
            String segmentsList = HadoopDruidIndexerConfig.JSON_MAPPER.readValue(res, JsonNode.class).get("segments").toString();
            List<DataSegment> dataSegments = HadoopDruidIndexerConfig.JSON_MAPPER.readValue(segmentsList,
                    new TypeReference<List<DataSegment>>() {
                    });
            return dataSegments;
        } catch (IOException e) {
            LOGGER.error("can not download segments info, reason: {}", e.getMessage());
            return ImmutableList.of();
        }
    }
}
