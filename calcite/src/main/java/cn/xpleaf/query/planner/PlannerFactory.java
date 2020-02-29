package cn.xpleaf.query.planner;

import cn.xpleaf.query.schema.InformationSchema;
import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.avatica.util.Quoting;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.config.CalciteConnectionConfigImpl;
import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.plan.Context;
import org.apache.calcite.plan.Contexts;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.validate.SqlConformanceEnum;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.Planner;
import org.apache.calcite.tools.Programs;

import java.util.Map;
import java.util.Properties;

public class PlannerFactory {

    private static final SchemaPlus ROOT_SCHEMA = CalciteSchema
            .createRootSchema(false, false).plus();

    private static final SqlParser.Config PARSER_CONFIG = SqlParser
            .configBuilder()
            .setCaseSensitive(false)
            .setQuoting(Quoting.BACK_TICK)
            .setUnquotedCasing(Casing.UNCHANGED)
            .setQuotedCasing(Casing.UNCHANGED)
            .setConformance(SqlConformanceEnum.DEFAULT)
            .build();

    static {
        ROOT_SCHEMA.add(InformationSchema.NAME, new InformationSchema(ROOT_SCHEMA));
    }

    private final PlannerConfig plannerConfig;

    public PlannerFactory(PlannerConfig plannerConfig) {
        this.plannerConfig = plannerConfig;
    }

    public Planner createPlanner(Map<String, Object> queryContext) {
        SqlToRelConverter.Config sqlToRelConverterConfig = SqlToRelConverter
                .configBuilder()
                .withExpand(false)
                .withTrimUnusedFields(true)
                .build();
        FrameworkConfig frameworkConfig = Frameworks
                .newConfigBuilder()
                .parserConfig(PARSER_CONFIG)
                .programs(Programs.standard())
                .context(Contexts.EMPTY_CONTEXT)
                .defaultSchema(ROOT_SCHEMA)
                .sqlToRelConverterConfig(sqlToRelConverterConfig)
                .context(new Context() {
                    @Override
                    @SuppressWarnings("unchecked")
                    public <C> C unwrap(Class<C> aClass) {
                        // This seems to be the best way to provide our own SqlConformance instance. Otherwise, Calcite's
                        // validator will not respect it.
                        if (aClass.equals(CalciteConnectionConfig.class)) {
                            final Properties properties = new Properties();
                            properties.setProperty(CalciteConnectionProperty.CASE_SENSITIVE.camelName(), String.valueOf(false));
                            queryContext.forEach((key, value) ->
                                    properties.setProperty(key, String.valueOf(value)));
                            return (C) new CalciteConnectionConfigImpl(properties);
                        }
                        return null;
                    }
                })
                .build();

        return Frameworks.getPlanner(frameworkConfig);
    }





}
