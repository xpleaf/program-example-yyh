package cn.xpleaf.query.schema;

import cn.xpleaf.query.table.ColumnType;
import cn.xpleaf.query.table.RowSignature;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.calcite.DataContext;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.schema.*;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.schema.impl.AbstractTable;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class InformationSchema extends AbstractSchema {

    public static final String NAME = "INFORMATION_SCHEMA";

    // INFORMATION_SCHEMA下的table
    private static final String SCHEMATA_TABLE = "SCHEMATA";
    private static final String TABLES_TABLE = "TABLES";

    // 构建Calcite RowType的signature
    private static final RowSignature SCHEMATA_SIGNATURE = RowSignature
            .builder()
            .add("SCHEMA_NAME", ColumnType.STRING)
            .add("DATASET_MODEL", ColumnType.STRING)
            .build();
    private static final RowSignature TABLES_SIGNATURE = RowSignature
            .builder()
            .add("TABLE_SCHEMA", ColumnType.STRING)
            .add("TABLE_NAME", ColumnType.STRING)
            .add("TABLE_TYPE", ColumnType.STRING)
            .build();
    private static final RelDataTypeSystem TYPE_SYSTEM = RelDataTypeSystem.DEFAULT;

    private final SchemaPlus rootSchema;
    private final Map<String, Table> tableMap;

    public InformationSchema(final SchemaPlus rootSchema) {
        this.rootSchema = rootSchema;
        this.tableMap = ImmutableMap.of(
                SCHEMATA_TABLE, new SchemaTable(),
                TABLES_TABLE, new TablesTable()
        );
    }

    @Override
    public Map<String, Table> getTableMap() {
        return tableMap;
    }

    class SchemaTable extends AbstractTable implements ScannableTable {

        @Override
        public Enumerable<Object[]> scan(DataContext root) {
            List<Object[]> results = rootSchema.getSubSchemaNames().stream().map(schemaName -> {
                final SchemaPlus subSchema = rootSchema.getSubSchema(schemaName);
                return new Object[]{
                        subSchema.getName(),    // SCHEMA_NAME
                        "no dataset model"      // DATASET_MODEL
                };
            }).collect(Collectors.toList());

            return Linq4j.asEnumerable(results);
        }

        @Override
        public RelDataType getRowType(RelDataTypeFactory typeFactory) {
            return SCHEMATA_SIGNATURE.getRelDataType(typeFactory);
        }

        @Override
        public Statistic getStatistic() {
            return Statistics.UNKNOWN;
        }

        @Override
        public TableType getJdbcTableType() {
            return TableType.SYSTEM_TABLE;
        }
    }

    class TablesTable extends AbstractTable implements ScannableTable {

        @Override
        public Enumerable<Object[]> scan(DataContext root) {
            ImmutableList.Builder<Object[]> builder = ImmutableList.builder();
            rootSchema.getSubSchemaNames().forEach(subSchemaName -> {
                SchemaPlus subSchema = rootSchema.getSubSchema(subSchemaName);
                Set<String> tableNames = subSchema.getTableNames();
                tableNames.forEach(tableName -> {
                    TableType tableType = subSchema.getTable(tableName).getJdbcTableType();
                    builder.add(new Object[]{
                            subSchemaName,  // TABLE_SCHEMA
                            tableName,      // TABLE_NAME
                            tableType       // TABLE_TYPE
                    });
                });
            });

            return Linq4j.asEnumerable(builder.build());
        }

        @Override
        public RelDataType getRowType(RelDataTypeFactory typeFactory) {
            return TABLES_SIGNATURE.getRelDataType(typeFactory);
        }

        @Override
        public Statistic getStatistic() {
            return Statistics.UNKNOWN;
        }

        @Override
        public TableType getJdbcTableType() {
            return TableType.SYSTEM_TABLE;
        }
    }
}
