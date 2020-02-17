package cn.xpleaf.query.datasource;

import cn.xpleaf.query.ResultSetUtil;
import cn.xpleaf.query.schema.InformationSchema;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.calcite.adapter.elasticsearch.ElasticsearchSchema;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.sql.*;
import java.util.Properties;

public class ElasticsearchQueryTest {

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
        rootSchema.add("es", buildElasticsearchSchema());
        statement = calciteConnection.createStatement();
    }

    private ElasticsearchSchema buildElasticsearchSchema() {
        RestClient restClient = RestClient
                .builder(new HttpHost("localhost", 9200))
                .build();
        return new ElasticsearchSchema(
                restClient, new ObjectMapper(), "teachers");
    }

    // demo data for es:
    // {name=xpleaf, age=26, rate=0.86, percent=0.95, join_time=1551058601000}
    @Test
    public void test_query() throws SQLException {
        String sql = "select * from es.teachers";
        resultSet = statement.executeQuery(sql);
        String res = ResultSetUtil.resultString(resultSet);
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
