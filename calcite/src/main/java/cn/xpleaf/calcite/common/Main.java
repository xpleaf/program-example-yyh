package cn.xpleaf.calcite.common;

import cn.xpleaf.calcite.schema.HrSchema;
import org.apache.calcite.adapter.java.ReflectiveSchema;
import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.avatica.util.Quoting;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.config.CalciteConnectionConfigImpl;
import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.plan.Context;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.sql.validate.SqlValidatorWithHints;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;

import java.util.Properties;

public class Main {

    public static void main(String[] args) throws Exception {
        SchemaPlus rootSchema = Frameworks.createRootSchema(true);
        rootSchema.add("hr", new ReflectiveSchema(new HrSchema()));


        SqlParser.Config sqlParserConfig = SqlParser.configBuilder()
                // 表示在sql中，引用字段或其他变量时，可以使用什么符号括起来，比如反引号括起来，如select `name` from `hr`.`emps`（但是不管怎么设置，解析之后的sqlNode，都是使用反引号表示）
                .setQuoting(Quoting.BACK_TICK)
                // 表示在sql中，对于没有使用Quoting括起来的字段，parse之后的转换规则，Casing.TO_UPPER表示会将其转换为大写
                .setUnquotedCasing(Casing.TO_UPPER)
                // 表示在sql中，对于没有使用了Quoting括起来的字段，parse之后的转换规则，Casing.TO_LOWER表示会将其转换为小写
                .setQuotedCasing(Casing.TO_LOWER)
                .setCaseSensitive(false)
                .build();

        FrameworkConfig frameworkConfig = Frameworks.newConfigBuilder()
                .parserConfig(SqlParser.Config.DEFAULT)
                .sqlToRelConverterConfig(SqlToRelConverter.Config.DEFAULT)
                .typeSystem(RelDataTypeSystem.DEFAULT)
                .operatorTable(SqlStdOperatorTable.instance())
                .defaultSchema(rootSchema)
                .build();


        // sql
        String sql = "select name from \"HR\".\"EMPS\"";
        System.out.println("Sql source: \n" + sql + "\n");

        // planner，use default rules
        VolcanoPlanner planner = new VolcanoPlanner(new Context() {
            @Override
            public <C> C unwrap(Class<C> aClass) {
                // This seems to be the best way to provide our own SqlConformance instance. Otherwise, Calcite's
                // validator will not respect it.
                final Properties props = new Properties();
                if (aClass.equals(CalciteConnectionConfig.class)) {
                    return (C) new CalciteConnectionConfigImpl(props);
                } else {
                    return null;
                }
            }
        });

        // sql parse
        SqlParser parser = SqlParser.create(sql, sqlParserConfig);
        SqlNode parsed = parser.parseStmt();
        System.out.println("Sql parsed: \n" + parsed + "\n");

        // sql validate
        JavaTypeFactoryImpl sqlTypeFactory = new JavaTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
        CalciteConnectionConfigImpl calciteConnectionConfig = new CalciteConnectionConfigImpl(new Properties());
        calciteConnectionConfig = calciteConnectionConfig.set(CalciteConnectionProperty.CASE_SENSITIVE, String.valueOf(sqlParserConfig.caseSensitive()));

        CalciteConnectionConfig connectionConfig = frameworkConfig.getContext().unwrap(CalciteConnectionConfig.class);


        CalciteCatalogReader calciteCatalogReader = new CalciteCatalogReader(
                CalciteSchema.from(rootSchema),
                CalciteSchema.from(rootSchema).path(null),
                sqlTypeFactory,
                calciteConnectionConfig);
        SqlValidatorWithHints validator = SqlValidatorUtil.newValidator(
                SqlStdOperatorTable.instance(),
                calciteCatalogReader,
                sqlTypeFactory,
                calciteConnectionConfig.conformance());
        SqlNode validated = validator.validate(parsed);
        System.out.println("Sql validated: \n" + validated + "\n");



    }


}
