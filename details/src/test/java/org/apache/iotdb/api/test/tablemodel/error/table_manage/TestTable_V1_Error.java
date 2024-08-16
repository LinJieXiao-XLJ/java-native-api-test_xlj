package org.apache.iotdb.api.test.tablemodel.error.table_manage;

import org.apache.iotdb.api.test.BaseTestSuite_TableModel;
import org.apache.iotdb.api.test.utils.CustomDataProvider;
import org.apache.iotdb.isession.SessionDataSet;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Title：测试表操作—异常情况
 * Describe：基于表模型V1版本，测试表所有的操作，包括创建表、查看表、查看表结构
 * Author：肖林捷
 * Date：2024/8/8
 */
public class TestTable_V1_Error extends BaseTestSuite_TableModel {
    /**
     * 创建测试环境
     */
    @BeforeClass
    public void afterTest() throws IoTDBConnectionException, StatementExecutionException {
        // 创建数据库
        try {
            session.executeNonQueryStatement("create database TestTable");
        } catch (Exception e) {
            session.executeNonQueryStatement("drop database TestTable");
            session.executeNonQueryStatement("create database TestTable");
        }
        // 使用数据库
        session.executeNonQueryStatement("use TestTable");
    }

    /**
     * 获取正确的数据并解析文档
     */
    @DataProvider(name = "table")
    public Iterator<Object[]> getData() throws IOException {
        return new CustomDataProvider().load_table("data/tableModel/table_error.csv", true).getData();
    }

    /**
     * 测试创建非法表
     */
    @Test(priority = 10) // 测试执行的优先级为10
    public void createTable() throws  IOException {
        // 获取解析后的数据
        for (Iterator<Object[]> it = getData(); it.hasNext(); ) {
            // 获取每行创建表的SQL语句
            Object[] tableSQLs = it.next();
            // 获取每行每列的数据
            for (Object tableSQL : tableSQLs) {
                try {
                    // 创建表
                    session.executeNonQueryStatement((String) tableSQL);
                } catch (Exception e) {
                    // 若不符合预期则断言
                    assert "org.apache.iotdb.rpc.StatementExecutionException".equals(e.getClass().getName()) : "TestTable_V1_Error的createTable 测试失败-其他错误:" + e;
                }
            }
        }
    }

    /**
     * 测试查看表——语法错误
     */
    @Test(priority = 20) // 测试执行的优先级为10
    public void showTables() throws IoTDBConnectionException, StatementExecutionException, IOException {
        try {
            // 查看表
            session.executeNonQueryStatement("SHOWTABLES");
        } catch (Exception e) {
            // 若不符合预期则断言
            assert "org.apache.iotdb.rpc.StatementExecutionException".equals(e.getClass().getName()) : "TestTable_V1_Error的showTables 测试失败-其他错误:" + e;
        }
    }

    /**
     * 测试查看表结构——语法错误和不存在的
     */
    @Test(priority = 30) // 测试执行的优先级为10
    public void describe() throws IoTDBConnectionException, StatementExecutionException, IOException {

        try {
            // 查看表结构-语法错误
            session.executeNonQueryStatement("describetable ");
            session.executeNonQueryStatement("desctable");
            // 不存在的
            session.executeNonQueryStatement("desc nonono");
            session.executeNonQueryStatement("describe nonono");
        } catch (Exception e) {
            // 若不符合预期则断言
            assert "org.apache.iotdb.rpc.StatementExecutionException".equals(e.getClass().getName()) : "TestTable_V1_Error的describe 测试失败-其他错误:" + e;
        }
    }

    /**
     * 清空测试环境
     */
    @AfterClass
    public void beforeTest() throws IoTDBConnectionException, StatementExecutionException {
        // 创建数据库
        session.executeNonQueryStatement("drop database TestTable");
    }
}
