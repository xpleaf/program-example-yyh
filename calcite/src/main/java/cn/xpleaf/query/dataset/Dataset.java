package cn.xpleaf.query.dataset;

import cn.xpleaf.query.table.ColumnType;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.Getter;
import lombok.ToString;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.List;

@Getter
@ToString
public class Dataset {

    private final String modelVersion;
    private final String name;
    private final List<Column> schema;
    private final Storage storage;

    private static final ObjectMapper mapper = new ObjectMapper();

    public static Dataset constructDataset(String datasetJson) throws IOException {
        return mapper.readValue(datasetJson, Dataset.class);
    }

    // 当有构造方法存在时，必须要有JsonCreator注解，同时必须要有JsonProperty注解，否则反序列化失败
    @JsonCreator
    private Dataset(@JsonProperty("modelVersion") String modelVersion,
                   @JsonProperty("name") String name,
                   @JsonProperty("schema") List<Column> schema,
                   @JsonProperty("storage") Storage storage) {
        this.modelVersion = modelVersion;
        this.name = name;
        this.schema = schema;
        this.storage = storage;
    }

    @Getter
    @ToString
    private static class Column {
        private final String name;
        private final ColumnType columnType;
        private final LinkedHashMap<String ,String> properties;

        @JsonCreator
        public Column(@JsonProperty("name") String name,
                      @JsonProperty("columnType") String columnType,
                      @JsonProperty("properties") LinkedHashMap<String ,String> properties) {
            this.name = name;
            this.properties = properties;
            this.columnType = formatColumnType(columnType);
        }

        private ColumnType formatColumnType(String columnType) {
            switch (columnType.toUpperCase()) {
                case "DOUBLE":
                    return ColumnType.DOUBLE;
                case "FLOAT":
                    return ColumnType.FLOAT;
                case "LONG":
                    if (properties != null
                            && "TIMESTAMP".equalsIgnoreCase(properties.getOrDefault("type", ""))) {
                        return ColumnType.TIMESTAMP;
                    }
                    return ColumnType.LONG;
                default:
                    return ColumnType.STRING;
            }
        }
    }

    @Getter
    @ToString
    private static class Storage {
        private final String type;
        private final String datasource;

        @JsonCreator
        public Storage(@JsonProperty("type") String type,
                       @JsonProperty("datasource") String datasource) {
            this.type = type;
            this.datasource = datasource;
        }
    }
}
