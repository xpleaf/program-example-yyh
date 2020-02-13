package cn.xpleaf.query.table;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.Pair;

import java.util.*;

public class RowSignature {

    private final Map<String, ColumnType> columnTypes;
    private final List<String> columnNames;

    private RowSignature(final List<Pair<String, ColumnType>> columnTypeList) {
        final Map<String, ColumnType> columnTypes0 = new HashMap<>();
        final ImmutableList.Builder<String> columnNamesBuilder = ImmutableList.builder();

        for (Pair<String, ColumnType> pair: columnTypeList) {
            final ColumnType existingType = columnTypes0.get(pair.left);
            if (existingType != null && existingType != pair.right) {
                String errorMessage = String.format(Locale.ENGLISH,
                        "Column[%s] has conflicting types [%s] and [%s]", pair.left, existingType, pair.right);
                throw new RuntimeException(errorMessage);
            }

            columnTypes0.put(pair.left, pair.right);
            columnNamesBuilder.add(pair.left);
        }

        this.columnTypes = ImmutableMap.copyOf(columnTypes0);
        this.columnNames = columnNamesBuilder.build();
    }

    /**
     * Returns a Calcite RelDataType corresponding to this row signature.
     *
     * @param typeFactory factory for type construction
     *
     * @return Calcite row type
     */
    public RelDataType getRelDataType(final RelDataTypeFactory typeFactory) {
        final RelDataTypeFactory.Builder builder = typeFactory.builder();
        columnNames.forEach(columnName -> {
            ColumnType columnType = columnTypes.get(columnName);
            switch (columnType) {
                case DOUBLE:
                    builder.add(columnName, SqlTypeName.DOUBLE);
                case FLOAT:
                    builder.add(columnName, SqlTypeName.FLOAT);
                case LONG:
                    builder.add(columnName, SqlTypeName.BIGINT);
                case TIMESTAMP:
                    builder.add(columnName, SqlTypeName.TIMESTAMP);
                case STRING:
                default:
                    builder.add(columnName, SqlTypeName.VARCHAR);
            }
        });

        return builder.build();
    }

    public ColumnType getColumnType(final String name) {
        return columnTypes.get(name);
    }

    /**
     * Returns the rowOrder for this signature, which is the list of column names in row order.
     *
     * @return row order
     */
    public List<String> getRowOrder() {
        return columnNames;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private final List<Pair<String, ColumnType>> columnTypeList;

        private Builder() {
            this.columnTypeList = new ArrayList<>();
        }

        public Builder add(String columnName, ColumnType columnType) {
            Objects.requireNonNull(columnName, "columnName");
            Objects.requireNonNull(columnType, "columnType");

            columnTypeList.add(Pair.of(columnName, columnType));
            return this;
        }

        public RowSignature build() {
            return new RowSignature(columnTypeList);
        }
    }
}
