package cn.xpleaf.calcite.jdbc;

import cn.xpleaf.calcite.schema.HrSchema;
import org.apache.calcite.adapter.java.ReflectiveSchema;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.runtime.Hook;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.util.Holder;

import java.sql.*;
import java.util.*;
import java.util.function.Consumer;

/**
 * Calcite内部完整封装和实现细节可以看这个来debug，基于此可以自行不断抽取一些代码和接口，以实现更细节的定制
 * 关于此，也可参考Apache Druid SQL的源码
 * https://github.com/apache/druid/blob/master/sql/src/main/java/org/apache/druid/sql
 *
 */
public class JdbcMain {

    public static void main(String[] args) throws Exception {
        Hook.ENABLE_BINDABLE.add((Consumer<Holder<Boolean>>) holder -> holder.set(true));
        Properties info = new Properties();
        info.setProperty("lex", "MYSQL");
        Connection connection = DriverManager.getConnection("jdbc:calcite:", info);
        CalciteConnection calciteConnection =
                connection.unwrap(CalciteConnection.class);
        final SchemaPlus rootSchema = calciteConnection.getRootSchema();
        rootSchema.add("hr", new ReflectiveSchema(new HrSchema()));
        Statement statement = calciteConnection.createStatement();

        final ResultSet resultSet =
                statement.executeQuery("select name as total from hr.emps");

/*        ResultSetFormatter resultSetFormatter = new ResultSetFormatter();
        resultSetFormatter.resultSet(resultSet);

        System.out.println(resultSetFormatter.string());

        System.out.println(resultSet);*/
        System.out.println(convertList(resultSet));
    }

    private static List convertList(ResultSet rs) throws SQLException {
        List list = new ArrayList();
        ResultSetMetaData md = rs.getMetaData();//获取键名
        int columnCount = md.getColumnCount();//获取行的数量
        while (rs.next()) {
            Map rowData = new HashMap();//声明Map
            for (int i = 1; i <= columnCount; i++) {
                rowData.put(md.getColumnName(i), rs.getObject(i));//获取键名及值
            }
            list.add(rowData);
        }
        return list;
    }

}
