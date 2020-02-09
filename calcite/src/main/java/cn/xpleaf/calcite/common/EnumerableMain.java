package cn.xpleaf.calcite.common;

import cn.xpleaf.calcite.schema.HrSchema;
import org.apache.calcite.DataContext;
import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.adapter.enumerable.EnumerableInterpretable;
import org.apache.calcite.adapter.enumerable.EnumerableRel;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.adapter.java.ReflectiveSchema;
import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.avatica.util.Quoting;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.config.CalciteConnectionConfigImpl;
import org.apache.calcite.jdbc.CalcitePrepare;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.QueryProvider;
import org.apache.calcite.plan.*;
import org.apache.calcite.prepare.CalcitePrepareImpl;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.runtime.Bindable;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.impl.SqlParserImpl;
import org.apache.calcite.sql.validate.SqlConformance;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.Planner;
import org.apache.calcite.tools.Programs;

import java.util.*;

public class EnumerableMain {

    public static void main(String[] args) throws Exception {
        SchemaPlus rootSchema = Frameworks.createRootSchema(true);
        rootSchema.add("hr", new ReflectiveSchema(new HrSchema()));

        SqlParser.Config sqlParserConfig = SqlParser.configBuilder()
                .setQuoting(Quoting.BACK_TICK)
                .setUnquotedCasing(Casing.UNCHANGED)
                .setQuotedCasing(Casing.UNCHANGED)
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
        // sql = "select name from hr.emps";
        System.out.println("Sql source: \n" + sql + "\n");

        // sql parse
        SqlNode parsed = commonPlanner.parse(sql);
        System.out.println("Sql parsed: \n" + parsed + "\n");

        // sql validate
        SqlNode validated = commonPlanner.validate(parsed);
        System.out.println("Sql validated: \n" + validated + "\n");

        // logicalPlan
        RelRoot root = commonPlanner.rel(validated);
        System.out.println("LogicalPlan: \n" + RelOptUtil.toString(root.rel) + "\n");

        // Optimize（PhysicalPlan）参考：org.apache.calcite.prepare.Prepare.optimize
        RelTraitSet relTraitSet = root.rel.getTraitSet()
                .replace(EnumerableConvention.INSTANCE) // add
//                .replace(BindableConvention.INSTANCE)   // add
                .replace(root.collation)    // add
                .simplify();
        List<RelOptMaterialization> materializationList = new ArrayList<>();
        List<RelOptLattice> latticeList = new ArrayList<>();
        RelOptPlanner planner = root.rel.getCluster().getPlanner();
        RelNode relNode = Programs.standard()
                .run(planner, root.rel, relTraitSet, materializationList, latticeList);
        root = root.withRel(relNode);
        System.out.println("PhysicalPlan: \n" + RelOptUtil.toString(root.rel) + "\n");

        // execute enumerable
        EnumerableRel enumerable = (EnumerableRel) root.rel;
        CalciteConnectionConfig calciteConnectionConfig = planner
                .getContext()
                .unwrap(CalciteConnectionConfig.class);
        SqlConformance conformance = calciteConnectionConfig
                .conformance();
        LinkedHashMap internalParameters = new LinkedHashMap();
        CalcitePrepare.SparkHandler sparkHandler = CalcitePrepareImpl.Dummy.getSparkHandler(false);
        internalParameters.put("_conformance", conformance);
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
                return null;
            }
        });
        System.out.println(bind);
        Iterator iterator = bind.iterator();
        while (iterator.hasNext()) {
            Object res = iterator.next();
            System.out.println(res);
        }
    }

    private static void implementEnumerable() {

    }

}
