package cn.xpleaf.calcite.result;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactoryImpl;
import org.apache.calcite.rel.type.RelDataTypeField;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

public class ResultFormatter {

    Iterator<Object[]> iterator;

    RelDataType rowType;

    List<Accessor> accessorList;

    List<Object> resultList = new LinkedList<>();

    public ResultFormatter(Iterator<Object[]> iterator, RelDataType rowType) {
        this.iterator = iterator;
        this.rowType = rowType;
        this.accessorList = createAccessors(rowType);
    }

    public List<Object> getResultList() {
        iterator.forEachRemaining(row -> {
            List<Object> rowList = new LinkedList<>();
            for (int i = 0; i < row.length; i++) {
                rowList.add(accessorList.get(i).getValue(row[i]));
            }
            resultList.add(rowList);
        });
        return resultList;
    }

    private static List<Accessor> createAccessors(RelDataType relDataType) {
        List<Accessor> accessorList = new LinkedList<>();
        List<RelDataTypeField> fieldList = relDataType.getFieldList();
        fieldList.forEach(relDataTypeField -> {
            RelDataType fieldType = relDataTypeField.getType();
            // TODO
            // 修复count(*)无法将BasicSqlType转换为JavaType的bug
            // 列计算或聚合函数时就不会是JavaType
            Class javaClass = ((RelDataTypeFactoryImpl.JavaType) fieldType).getJavaClass();
            if (String.class.equals(javaClass)) {
                accessorList.add(new StringAccessor());
            } else if (Long.class.equals(javaClass)) {
                accessorList.add(new LongAccessor());
            } else {
                accessorList.add(new DefaultAccessor());
            }
        });
        return accessorList;
    }

    private interface Accessor {

        Object getValue(Object obj);

    }


    private static class StringAccessor implements Accessor {

        @Override
        public Object getValue(Object obj) {
            if (obj instanceof String) {
                return (String) obj;
            }
            return null == obj ? null : obj.toString();
        }
    }

    private static class LongAccessor implements Accessor {

        @Override
        public Object getValue(Object obj) {
            Long o = (Long) obj;
            return o == null ? 0 : o;
        }
    }

    private static class DefaultAccessor implements Accessor {

        @Override
        public Object getValue(Object obj) {
            return obj;
        }
    }

}
