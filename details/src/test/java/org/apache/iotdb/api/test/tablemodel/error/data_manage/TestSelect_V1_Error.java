package org.apache.iotdb.api.test.tablemodel.error.data_manage;

import org.apache.iotdb.api.test.BaseTestSuite_TableModel;
import org.apache.iotdb.api.test.utils.CustomDataProvider;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.Iterator;

/**
 * Title：测试数据查询—异常情况
 * Describe：基于表模型V1版本，测试各种方式的数据查询的操作
 * Author：肖林捷
 * Date：2024/8/12
 */
public class TestSelect_V1_Error extends BaseTestSuite_TableModel {
    /**
     * 创建测试环境
     */
    @BeforeClass
    public void afterTest() throws IoTDBConnectionException, StatementExecutionException {
        // 创建数据库
        try {
            session.executeNonQueryStatement("create database TestSelect");
        } catch (Exception e) {
            session.executeNonQueryStatement("drop database TestSelect");
            session.executeNonQueryStatement("create database TestSelect");
        }
        // 使用数据库
        session.executeNonQueryStatement("use TestSelect");
        // 创建表
        session.executeNonQueryStatement("create table device_type_a" +
                " ( " +
                "    \"地区\" STRING ID," +
                "    \"厂号\" STRING ID," +
                "    \"设备号\" STRING ID," +
                "    \"型号\" STRING ATTRIBUTE," +
                "    \"温度\" FLOAT MEASUREMENT," +
                "    \"排量\" DOUBLE MEASUREMENT" +
                " ) " +
                "with (TTL=3600000)");
        // 插入数据
        session.executeNonQueryStatement("insert into device_type_a(time, \"地区\", \"厂号\", \"设备号\", \"型号\",  \"温度\", \"排量\") values(1, '湖南', '1002', '设备2', '型号2', 20.0, 1200.0),(2, '湖南', '1003', '设备3', '型号3', 30.0, 1300.0)");
    }

    /**
     * 获取不正确的数据并解析文档
     */
    @DataProvider(name = "select")
    public Iterator<Object[]> getData() throws IOException {
        return new CustomDataProvider().load_table("data/tableModel/select_error.csv", true).getData();
    }


    /**
     * 测试查询数据
     */
    @Test(priority = 10) // 测试执行的优先级为10
    public void select() throws IoTDBConnectionException, StatementExecutionException, IOException {
        // 获取解析后的数据
        for (Iterator<Object[]> it = getData(); it.hasNext(); ) {
            // 获取每行的SQL语句
            Object[] selectSQLs = it.next();
            // 获取每行每列的数据
            for (Object selectSQL : selectSQLs) {
                try {
                    // 查询数据
                    session.executeNonQueryStatement((String) selectSQL);
                } catch (Exception e) {
                    // 若不符合预期则断言
                    assert "org.apache.iotdb.rpc.StatementExecutionException".equals(e.getClass().getName()) : "TestSelect_V1_Error 的 selectSQL 测试失败-其他错误:" + e;
                }
            }
        }
    }

    /**
     * 清空测试环境
     */
    @AfterClass
    public void beforeTest() throws IoTDBConnectionException, StatementExecutionException {
        // 创建数据库
        session.executeNonQueryStatement("drop database TestSelect");
    }
}
