package cn.xpleaf.query.schema;

import cn.xpleaf.query.table.ColumnType;
import cn.xpleaf.query.table.RowSignature;
import org.apache.calcite.schema.impl.AbstractSchema;

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


}
