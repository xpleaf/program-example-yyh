package cn.xpleaf.query.datasource;

import cn.xpleaf.query.ResultSetUtil;
import cn.xpleaf.query.schema.InformationSchema;
import org.apache.calcite.adapter.druid.DruidSchema;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.schema.SchemaPlus;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.sql.*;
import java.util.Properties;

public class DruidQueryTest {

    Connection connection;
    Statement statement;
    ResultSet resultSet;

    @Before
    public void init() throws SQLException {
        Properties info = new Properties();
        info.setProperty("lex", "MYSQL");
        connection = DriverManager.getConnection("jdbc:calcite:", info);
        CalciteConnection calciteConnection =
                connection.unwrap(CalciteConnection.class);
        final SchemaPlus rootSchema = calciteConnection.getRootSchema();
        rootSchema.add("information_schema", new InformationSchema(rootSchema));
        rootSchema.add("druid", buildDruidSchema());
        statement = calciteConnection.createStatement();
    }

    private DruidSchema buildDruidSchema() {
        String brokerUrl = "http://localhost:8082";
        String coordinatorUrl = "http://localhost:8081";
        return new DruidSchema(brokerUrl, coordinatorUrl, true);
    }

    @Test
    public void test_query() throws SQLException {
        String sql = "select * from druid.wiki limit 1";
        resultSet = statement.executeQuery(sql);
        String res = ResultSetUtil.resultString(resultSet, true);
        System.out.println(res);

        sql = "select * from information_schema.schemata";
        resultSet = statement.executeQuery(sql);
        res = ResultSetUtil.resultString(resultSet, true);
        System.out.println(res);

        sql = "select * from information_schema.tables";
        resultSet = statement.executeQuery(sql);
        res = ResultSetUtil.resultString(resultSet, true);
        System.out.println(res);
    }

    @After
    public void clear() throws SQLException {
        resultSet.close();
        statement.close();
        connection.close();
    }

}
