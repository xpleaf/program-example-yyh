package cn.xpleaf.query.table;

/**
 * 业务层面的数据类型
 */
public enum ColumnType {

    STRING("STRING"),
    DOUBLE("DOUBLE"),
    FLOAT("FLOAT"),
    LONG("LONG"),
    TIMESTAMP("TIMESTAMP");

    private String columnType;

    ColumnType(String columnType) {
        this.columnType = columnType;
    }

    public String getColumnType() {
        return columnType;
    }
}
