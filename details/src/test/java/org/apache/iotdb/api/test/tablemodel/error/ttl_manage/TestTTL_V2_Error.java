package org.apache.iotdb.api.test.tablemodel.error.ttl_manage;

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
 * Title：测试TTL操作—异常情况
 * Describe：基于表模型V1版本，测试TTL所有的操作，包括创建TTL
 * Author：肖林捷
 * Date：2024/8/8
 */
public class TestTTL_V2_Error extends BaseTestSuite_TableModel {

    /**
     * 创建测试环境
     */
    @BeforeClass
    public void afterTest() throws IoTDBConnectionException, StatementExecutionException {
        // 创建数据库
        try {
            session.executeNonQueryStatement("create database TestTTL");
        } catch (Exception e) {
            session.executeNonQueryStatement("drop database TestTTL");
            session.executeNonQueryStatement("create database TestTTL");
        }
        // 使用数据库
        session.executeNonQueryStatement("use TestTTL");
    }

    /**
     * 获取不正确的数据并解析文档
     */
    @DataProvider(name = "ttl")
    public Iterator<Object[]> getData() throws IOException {
        return new CustomDataProvider().load_table("data/tableModel/ttl_error.csv", false).getData();
    }

    /**
     * 测试设置TTL
     */
    @Test(priority = 10) // 测试执行的优先级为10
    public void setTTL() throws IOException {
        // 前缀表名
        String prefixTableName = "tableName";
        // 后缀表号
        int num = 1;
        // 获取解析后的数据
        for (Iterator<Object[]> it = getData(); it.hasNext(); ) {
            // 获取每行数据
            Object[] ttls = it.next();
            // 组合一个表名
            String tableName = prefixTableName + (num++);
            for (Object ttl : ttls) {
                try {
                    // 建表时设置TTL
                    session.executeNonQueryStatement("create table " + tableName +
                            " (" +
                            "\"地区\" STRING ID, " +
                            "\"厂号\" STRING ID, " +
                            "\"设备号\" STRING ID, " +
                            "\"温度\" FLOAT MEASUREMENT, " +
                            "\"排量\" DOUBLE MEASUREMENT" +
                            ")" +
                            " with " +
                            "(TTL=" + ttl + ")");
                } catch (Exception e) {
                    // 若不符合预期则断言
                    assert "org.apache.iotdb.rpc.StatementExecutionException".equals(e.getClass().getName()) : "TestTTL_V1_Error的setTTL 测试失败-其他错误:" + e;
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
        session.executeNonQueryStatement("drop database TestTTL");
    }
}
