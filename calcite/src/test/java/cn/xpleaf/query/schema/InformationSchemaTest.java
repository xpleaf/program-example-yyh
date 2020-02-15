package cn.xpleaf.query.schema;

import cn.xpleaf.query.ResultSetUtil;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.schema.SchemaPlus;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.sql.*;
import java.util.List;
import java.util.Properties;

public class InformationSchemaTest {

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
        statement = calciteConnection.createStatement();
    }

    @Test
    public void test_schemata() throws SQLException {
        String sql = "select * from information_schema.schemata";
        resultSet = statement.executeQuery(sql);
        // information_schema and metadata
        // metadata为calcite自动为rootSchema构建的元数据信息schema
        List<List<Object>> resultList = ResultSetUtil.resultList(resultSet);
        Assert.assertEquals(2, resultList.size());
        Assert.assertTrue(resultList.toString().contains("information_schema"));
        Assert.assertTrue(resultList.toString().contains("metadata"));

        sql = "select * from information_schema.schemata where schema_name='information_schema'";
        resultSet = statement.executeQuery(sql);
        resultList = ResultSetUtil.resultList(resultSet);
        Assert.assertEquals(1, resultList.size());
        Assert.assertTrue(resultList.toString().contains("information_schema"));
    }

    @Test
    public void test_tables() throws SQLException {
        String sql = "select * from information_schema.tables";
        resultSet = statement.executeQuery(sql);
        // information_schema下的table and metadata下的table
        List<List<Object>> resultList = ResultSetUtil.resultList(resultSet);
        Assert.assertEquals(4, resultList.size());
        Assert.assertTrue(resultList.toString().contains("SCHEMATA"));
        Assert.assertTrue(resultList.toString().contains("TABLES"));

        sql = "select * from information_schema.tables where table_schema='information_schema'";
        resultSet = statement.executeQuery(sql);
        resultList = ResultSetUtil.resultList(resultSet);
        Assert.assertEquals(2, resultList.size());
    }

    @Test
    public void test_print() throws SQLException {
        String sql = "select * from information_schema.schemata";
        resultSet = statement.executeQuery(sql);
        String res = ResultSetUtil.resultString(resultSet, true);
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
