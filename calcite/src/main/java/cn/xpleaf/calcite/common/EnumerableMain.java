package cn.xpleaf.calcite.common;

import cn.xpleaf.calcite.schema.HrSchema;
import cn.xpleaf.calcite.schema.StaffSchema;
import cn.xpleaf.calcite.utils.ResultSetUtil;
import cn.xpleaf.query.schema.InformationSchema;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.DataContext;
import org.apache.calcite.adapter.druid.DruidSchema;
import org.apache.calcite.adapter.elasticsearch.ElasticsearchSchema;
import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.adapter.enumerable.EnumerableInterpretable;
import org.apache.calcite.adapter.enumerable.EnumerableRel;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.adapter.java.ReflectiveSchema;
import org.apache.calcite.adapter.jdbc.JdbcConvention;
import org.apache.calcite.adapter.jdbc.JdbcSchema;
import org.apache.calcite.adapter.jdbc.JdbcTable;
import org.apache.calcite.adapter.jdbc.JdbcTableScan;
import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.avatica.util.Quoting;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.config.CalciteConnectionConfigImpl;
import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.jdbc.CalcitePrepare;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.QueryProvider;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.plan.*;
import org.apache.calcite.prepare.CalcitePrepareImpl;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.runtime.Bindable;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Schemas;
import org.apache.calcite.schema.Table;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.dialect.HiveSqlDialect;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.impl.SqlParserImpl;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.util.SqlShuttle;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.Planner;
import org.apache.calcite.tools.Programs;
import org.apache.calcite.util.Pair;
import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.commons.dbcp2.BasicDataSourceFactory;
import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;

import javax.sql.DataSource;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.*;

public class EnumerableMain {

    private static RestClient restClient;

    public static void main(String[] args) throws Exception {
        SchemaPlus rootSchema = Frameworks.createRootSchema(true);
        rootSchema.add("information_schema", new InformationSchema(rootSchema));
        rootSchema.add("hr", new ReflectiveSchema(new HrSchema()));
        rootSchema.add("staff", new ReflectiveSchema(new StaffSchema()));
        rootSchema.add("druid", buildDruidSchema());
        rootSchema.add("es", buildElasticsearchSchema());
        // Note: 实际上查hive或者rdb，不建议使用下面的这种方式，按照传统jdbc方式直接拿到sql来透传就好了，因为calcite的jdbc适配器
        // 并不太好，稍微复杂一点的sql语句就会导致无法pushdown，进而导致大量的操作在calcite层面完成，如果数据量小那倒没啥，
        // 但是如果是查询hive或者Spark Thrift JDBCServer，把这么多数据load回来再做单机版的计算，那就很容易OOM
        // hive或spark本身的分布式意义就不是很大了，这时它仅仅用来scan数据
        // 所以要考虑sql的优化，我们把validate之后的sql透传给hive或spark sql，让它使用自身的sql优化器去优化并生成物理计划就好了
        buildJdbcSchemaForHive(rootSchema);
        buildJdbcSchemaForMysql(rootSchema);

        SqlParser.Config sqlParserConfig = SqlParser.configBuilder()
                .setQuoting(Quoting.BACK_TICK)
                .setUnquotedCasing(Casing.UNCHANGED)
                .setQuotedCasing(Casing.UNCHANGED)
                // 不会有效地设置到Validator中，因为此时使用下面unwrap的CalciteConnectionConfig，而不是在PlannerImpl中新创建一个
                .setCaseSensitive(false)
                .setParserFactory(SqlParserImpl::new)
                .build();

        FrameworkConfig frameworkConfig = Frameworks.newConfigBuilder()
                .parserConfig(sqlParserConfig)
                .sqlToRelConverterConfig(SqlToRelConverter.Config.DEFAULT)
                .typeSystem(RelDataTypeSystem.DEFAULT)
                .operatorTable(SqlStdOperatorTable.instance())
                .defaultSchema(rootSchema)
                .context(new Context() {
                    @Override
                    public <C> C unwrap(Class<C> aClass) {
                        // This seems to be the best way to provide our own SqlConformance instance. Otherwise, Calcite's
                        // validator will not respect it.
                        final Properties props = new Properties();
                        props.setProperty(CalciteConnectionProperty.CASE_SENSITIVE.camelName(), String.valueOf(false));
                        if (aClass.equals(CalciteConnectionConfig.class)) {
                            return (C) new CalciteConnectionConfigImpl(props);
                        }
                        return null;
                    }
                })
                .build();

        // planner
        Planner commonPlanner = Frameworks.getPlanner(frameworkConfig);
        // 因为使用的是封装好的planner，所以它在加rule时需要判断Hook.ENABLE_BINDABLE（默认为false）是否为true在确定是否需要添加BindableRules的
        // 所以需要设置为true，否则设置Convention为Bindable时是无法完成优化的，here are not enough rules to produce a node with desired properties: convention=BINDABLE, sort=[].
        // 它添加rule的调用链为：parse:50,Planner -> parse:197,PlannerImpl -> ready:172,PlannerImpl -> registerDefaultRules:1834,RelOptUtil
        // ready:172,PlannerImpl中就传入了Hook.ENABLE_BINDABLE.get(false)
//        Hook.ENABLE_BINDABLE.add((Consumer<Holder<Boolean>>) holder -> holder.set(true));

        // sql
        String sql = "select * from hr.emps";
        sql = "select count(*) as total from hr.emps";
        sql = "select name,count(*) as total from hr.emps group by name";
        sql = "select name from hr.emps";
        sql = "select _MAP['name'] from es.teachers";
        sql = "select comment from druid.wiki limit 1";
        sql = "explain plan for " + sql;
        sql = "select schema_name from information_schema.schemata";
        sql = "select table_name from information_schema.tables";
        sql = "select * from hive.person order by salary desc limit 1";
        // Note: 下面这个sql，如果走calcite的全jdbc流程，只有scan能够pushdown，其它所有计算都在calcite层面完成
        // 所以识别出其是查询jdbc数据源，就不走全流程的calcite jdbc方式，拿到dataSource之后走传统的jdbc方式查询
        String complexSql = "select t.double_salary,t.young_age,(t.double_salary + t.young_age) * 2 as complex_field,count(*) as total\n" +
                "from\n" +
                "(\n" +
                "select salary * 2 as double_salary,age - 1 as young_age\n" +
                "from hive.person\n" +
                "order by age desc \n" +
                "limit 5\n" +
                ") as t\n" +
                "group by t.double_salary,t.young_age\n" +
                "order by complex_field desc \n" +
                "limit 1";
        sql = "select * from hive.person";
        sql = "select * from mysql.teacher";
        sql = complexSql;   // for hive test
        sql = complexSql.replace("hive.person", "mysql.teacher");   // for mysql test
        sql = "SELECT\n" +
                "    u.NAME,\n" +
                "    u.age,\n" +
                "    c.NAME AS user_company\n" +
                "FROM\n" +
                "    staff.users u\n" +
                "        JOIN staff.companies c ON u.company = c.id\n" +
                "WHERE\n" +
                "        u.age >= 26\n" +
                "ORDER BY\n" +
                "    u.age DESC";
        // sql parse
        SqlNode parsed = commonPlanner.parse(sql);
        System.out.println("Sql parsed: \n" + parsed + "\n");

        SqlExplain sqlExplain = null;
        if (parsed.getKind() == SqlKind.EXPLAIN) {
            sqlExplain = (SqlExplain) parsed;
            parsed = sqlExplain.getExplicandum();
        }

        // sql validate
        SqlNode validated = commonPlanner.validate(parsed);
        System.out.println("Sql validated: \n" + validated + "\n");

        // logicalPlan
        RelRoot root = commonPlanner.rel(validated);
        System.out.println("LogicalPlan: \n" + RelOptUtil.toString(root.rel) + "\n");
        Pair<Boolean, SqlDialect> pair = isJdbcQuery(root.rel);
        if (pair.left) {    // Note: jdbc查询，走传统的jdbc执行流程，calcite只用于元数据系统构建和schema校验
            System.out.println("it's a jdbc query! Type: " + pair.right.getClass().getSimpleName());
            rewriteSql(validated, rootSchema);
            String validatedSql = validated.toSqlString(pair.right).toString();
            JdbcSchema jdbcSchema = rootSchema.getSubSchema(getSchemaName(root.rel)).unwrap(JdbcSchema.class);
            DataSource dataSource = jdbcSchema.getDataSource();
            try(
            Connection connection = dataSource.getConnection();
            PreparedStatement preparedStatement = connection.prepareStatement(validatedSql);
            ResultSet resultSet = preparedStatement.executeQuery()) {
                List<List<Object>> lists = ResultSetUtil.resultList(resultSet);
                System.out.println(lists);
            }
            return;
        }

        // Optimize（PhysicalPlan）参考：org.apache.calcite.prepare.Prepare.optimize
        RelTraitSet relTraitSet = root.rel.getTraitSet()
                .replace(EnumerableConvention.INSTANCE) // add
//                .replace(BindableConvention.INSTANCE)   // add
                .replace(root.collation)    // add
                .simplify();
        // TODO Convention是如何影响Planer的优化的
        List<RelOptMaterialization> materializationList = new ArrayList<>();
        List<RelOptLattice> latticeList = new ArrayList<>();
        RelOptPlanner planner = root.rel.getCluster().getPlanner();
        RelNode relNode = Programs.standard()
                .run(planner, root.rel, relTraitSet, materializationList, latticeList);
        root = root.withRel(relNode);
        System.out.println("PhysicalPlan: \n" + RelOptUtil.toString(root.rel) + "\n");

        if (sqlExplain != null) {
            String res = RelOptUtil.toString(root.rel);
            System.out.println("Explain result: \n" + res + "\n");
        } else {
            // execute enumerable
            EnumerableRel enumerable = (EnumerableRel) root.rel;
            CalciteConnectionConfig calciteConnectionConfig = planner
                    .getContext()
                    .unwrap(CalciteConnectionConfig.class);
            LinkedHashMap internalParameters = new LinkedHashMap();
            CalcitePrepare.SparkHandler sparkHandler = CalcitePrepareImpl.Dummy.getSparkHandler(false);
            Bindable bindable = EnumerableInterpretable.toBindable(internalParameters,
                    sparkHandler, enumerable, EnumerableRel.Prefer.ARRAY);
            Enumerable bind = bindable.bind(new DataContext() {
                @Override
                public SchemaPlus getRootSchema() {
                    return rootSchema;
                }

                @Override
                public JavaTypeFactory getTypeFactory() {
                    return new JavaTypeFactoryImpl();
                }

                @Override
                public QueryProvider getQueryProvider() {
                    return null;
                }

                @Override
                public Object get(String name) {
                    // DataContext的较完整实现可以参考Calcite的JDBC流程方式源码来看，这里只是让整个流程跑起来
                    // 这里要注意的是，如果数据源为ES，这里直接return null也行，因为es的适配器的执行方式不会用到这里的参数
                    // 但是数据源为Druid时，它使用EnumerableInterpreter来执行：
                /*
                public org.apache.calcite.linq4j.Enumerable bind(final org.apache.calcite.DataContext root) {
                  final org.apache.calcite.rel.RelNode v0stashed = (org.apache.calcite.rel.RelNode) root.get("v0stashed");
                  final org.apache.calcite.interpreter.Interpreter interpreter = new org.apache.calcite.interpreter.Interpreter(
                    root,
                    v0stashed);
                  return org.apache.calcite.runtime.Enumerables.slice0(interpreter);
                }
                 */
                    // 它会从DataContext去获取优化后的物理计划的，其实也就是保存在前面的internalParameters当中
                    // 所以要把internalParameters中的k-v值添加到这里的context中来，只是为了简化，我直接用internalParameters
                    // 当作context，实际上还有其它一些值需要保存到context这里来的，请参考Calcite原生主流程源码
                    return internalParameters.get(name);
                }
            });
            System.out.println(bind);
            Iterator iterator = bind.iterator();
            while (iterator.hasNext()) {
                Object res = iterator.next();
                if (res instanceof Object[]) {
                    System.out.println(Arrays.toString((Object[]) res));
                } else {
                    System.out.println(res);
                }
            }
        }
        closeRestClient();
    }

    private static class SqlFromReWriterForJdbc extends SqlShuttle {


        private SchemaPlus rootSchema;

        SqlFromReWriterForJdbc(SchemaPlus rootSchema) {
            this.rootSchema = rootSchema;
        }

        @Override
        public SqlNode visit(SqlIdentifier id) {
            ImmutableList<String> names = id.names;
            int size = names.size();
            if (size > 1) {
                String schemaName = names.get(0);
                String tableName = names.get(1);
                Table table = rootSchema.getSubSchema(schemaName).getTable(tableName);
                if (table instanceof JdbcTable) {
                    JdbcTable jdbcTable = (JdbcTable) table;
                    String jdbcSchemaName = jdbcTable.jdbcSchemaName;
                    String jdbcTableName = jdbcTable.jdbcTableName;
                    id.setNames(
                            ImmutableList.of(jdbcSchemaName, jdbcTableName),
                            ImmutableList.of(id.getComponentParserPosition(0), id.getComponentParserPosition(1))
                    );
                }
            }
            return super.visit(id);
        }
    }

    private static void rewriteSql(SqlNode sqlNode, SchemaPlus rootSchema) {
        switch (sqlNode.getKind()) {
            case SELECT:
                rewriteSql(((SqlSelect) sqlNode).getFrom(), rootSchema);
                break;
            case AS:
                rewriteSql(((SqlBasicCall) sqlNode).operand(0), rootSchema);
                break;
            case IDENTIFIER:
                sqlNode.accept(new SqlFromReWriterForJdbc(rootSchema));
                break;
        }
    }

    private static String getSchemaName(RelNode rel) {
        if (rel.getInputs().size() > 0) {
            return getSchemaName(rel.getInput(0));
        } else {
            if (rel instanceof TableScan) {
                // qualifieldName为mysql.teacher或hive.person
                // index为0的就是calcite层面的schemaName
                return rel.getTable().getQualifiedName().get(0);
            }
            return null;
        }
    }

    private static Pair<Boolean, SqlDialect> isJdbcQuery(RelNode rel) {
        if (rel.getInputs().size() > 0) {
            return isJdbcQuery(rel.getInput(0));
        } else {
            if (rel instanceof JdbcTableScan) {
                JdbcTableScan jdbcTableScan = (JdbcTableScan) rel;
                JdbcSchema jdbcSchema = jdbcTableScan.jdbcTable.jdbcSchema;
                JdbcConvention convention = (JdbcConvention) rel.getConvention();
                return Pair.of(true, convention.dialect);
            }
        }
        return Pair.of(false, null);
    }

    private static DruidSchema buildDruidSchema() {
        String brokerUrl = "http://localhost:8082";
        String coordinatorUrl = "http://localhost:8081";
        return new DruidSchema(brokerUrl, coordinatorUrl, true);
    }

    private static ElasticsearchSchema buildElasticsearchSchema() {
        restClient = RestClient
                .builder(new HttpHost("localhost", 9200))
                .build();
        return new ElasticsearchSchema(
                restClient, new ObjectMapper(), "teachers");
    }

    private static void buildJdbcSchemaForHive(SchemaPlus rootSchema) throws Exception {
        Properties properties = new Properties();
        properties.setProperty("driverClassName", "org.apache.hive.jdbc.HiveDriver");
        properties.setProperty("url", "jdbc:hive2://localhost:10000");
        BasicDataSource dataSource = BasicDataSourceFactory.createDataSource(properties);
        // 为什么这样写？它用于Jdbc方式执行时的动态代码获取subSchema，你可以自己调试一下，这是参考JdbcSchema实现的
        String calciteHiveSchemaName = "hive";  // Note: 这是calcite层面的schemaName
        Expression expression = Schemas.subSchemaExpression(rootSchema, calciteHiveSchemaName, JdbcSchema.class);
        JdbcConvention hiveConversion = JdbcConvention.of(HiveSqlDialect.DEFAULT, expression, "hiveConversion");
        String hiveSchemaName = "default";  // Note: 这是hive元数据层面的schemaName，就是我们平时理解的db
        // Note: 下面创建JdbcSchema时的catalog要用null，否则转换为底层数据源的sql时，会变为*.default.person，因为你填写""的话
        // 它认为你是有catalog的，所以在前面加了*，但是如果填写null的话，就会认为没有catalog，这样底层数据源的顶层nameSpace就是default
        JdbcSchema jdbcSchema = new JdbcSchema(dataSource, HiveSqlDialect.DEFAULT, hiveConversion, null, hiveSchemaName);
        rootSchema.add(calciteHiveSchemaName, jdbcSchema);

        // 下面用反射的原因在于，JdbcTable的一些方法是package私有的，但为了构建定制的元数据系统，我们需要手动创建
        // 另外，还手动给table添加RowType，目前来看查hive去获取类型时拿不到，会导致生成的动态代码有问题，动态代码在执行被编译时会报错
        // 至于生成的动态代码是怎么样的，为什么会报错？报什么错？它是如何根据table的字段类型去生成的动态代码的？可以去debug一下代码，不复杂
        Class<JdbcTable> jdbcTableClass = JdbcTable.class;
        Constructor<JdbcTable> jdbcTableConstructor = jdbcTableClass.getDeclaredConstructor(JdbcSchema.class,
                String.class, String.class, String.class, Schema.TableType.class);
        jdbcTableConstructor.setAccessible(true);
        String hiveTableName = "person";    // Note: 这是hive层面的tableName
        JdbcTable jdbcTable = jdbcTableConstructor.newInstance(jdbcSchema, null, hiveSchemaName, hiveTableName, Schema.TableType.TABLE);

        Field protoRowType = jdbcTableClass.getDeclaredField("protoRowType");
        protoRowType.setAccessible(true);
        JavaTypeFactoryImpl typeFactory = new JavaTypeFactoryImpl();
        RelDataTypeFactory.Builder builder = typeFactory.builder();
        builder.add("name", SqlTypeName.VARCHAR);
        builder.add("age", SqlTypeName.INTEGER);
        builder.add("salary", SqlTypeName.DOUBLE);
        RelProtoDataType relProtoDataType = relDataTypeFactory -> builder.build();
        protoRowType.set(jdbcTable, relProtoDataType);

        String calciteHiveTableName = "person"; // Note: 这是calcite层面的tableName
        rootSchema.getSubSchema(calciteHiveSchemaName).add(calciteHiveTableName, jdbcTable);
    }


    private static void buildJdbcSchemaForMysql(SchemaPlus rootSchema) throws Exception {
        Properties properties = new Properties();
        properties.setProperty("driverClassName", "");
        properties.setProperty("url", "jdbc:mysql://localhost:3306");
        properties.setProperty("username", "root");
        properties.setProperty("password", "root123456");
        BasicDataSource dataSource = BasicDataSourceFactory.createDataSource(properties);
        JdbcSchema jdbcSchema = JdbcSchema.create(rootSchema, "mysql", dataSource, null, "school");
        // add to rootSchema
        rootSchema.add("mysql", jdbcSchema);

        // create jdbcTable
        Class<JdbcTable> jdbcTableClass = JdbcTable.class;
        Constructor<JdbcTable> jdbcTableConstructor = jdbcTableClass.getDeclaredConstructor(JdbcSchema.class,
                String.class, String.class, String.class, Schema.TableType.class);
        jdbcTableConstructor.setAccessible(true);
        JdbcTable jdbcTable = jdbcTableConstructor.newInstance(jdbcSchema, null, "school", "teacher", Schema.TableType.TABLE);

        Field protoRowType = jdbcTableClass.getDeclaredField("protoRowType");
        protoRowType.setAccessible(true);
        JavaTypeFactoryImpl typeFactory = new JavaTypeFactoryImpl();
        RelDataTypeFactory.Builder builder = typeFactory.builder();
        builder.add("name", SqlTypeName.VARCHAR);
        builder.add("age", SqlTypeName.INTEGER);
        builder.add("salary", SqlTypeName.DOUBLE);
        RelProtoDataType relProtoDataType = relDataTypeFactory -> builder.build();
        protoRowType.set(jdbcTable, relProtoDataType);

        // add to jdbcSchema
        rootSchema.getSubSchema("mysql").add("teacher", jdbcTable);
    }

    private static void closeRestClient() throws IOException {
        if (restClient != null) {
            restClient.close();
        }
    }
}
