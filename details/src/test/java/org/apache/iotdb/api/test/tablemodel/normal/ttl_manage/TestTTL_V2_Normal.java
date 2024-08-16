package org.apache.iotdb.api.test.tablemodel.normal.ttl_manage;

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

/**
 * Title：测试TTL操作—正常情况
 * Describe：基于表模型V2版本，测试TTL所有的操作，包括创建TTL、查看TTL
 * Author：肖林捷
 * Date：2024/8/8
 */
public class TestTTL_V2_Normal extends BaseTestSuite_TableModel {

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
     * 获取正确的数据并解析文档
     */
    @DataProvider(name = "ttl")
    public Iterator<Object[]> getData() throws IOException {
        return new CustomDataProvider().load_table("data/tableModel/ttl_normal.csv", false).getData();
    }

    /**
     * 测试设置TTL
     */
    @Test(priority = 10) // 测试执行的优先级为10
    public void setTTL() throws IoTDBConnectionException, StatementExecutionException, IOException {
        // 期待表结构数量
        int expect = 0;
        // 实际表结构数量（先默认未0）
        int actual = 0;
        // 前缀表名
        String prefixTableName = "table";
        // 后缀表号
        int num = 1;
        // 获取解析后的数据
        for (Iterator<Object[]> it = getData(); it.hasNext(); ) {
            // 获取每行数据
            Object[] ttls = it.next();
            // 组合一个表名
            String tableName = prefixTableName + (num++);
            // 获取每行每列的数据
            for (Object ttl : ttls) {
                expect++;
                // 建表时设置TTL
                session.executeNonQueryStatement("create table " + tableName + " (\"地区\" STRING ID, \"厂号\" STRING ID, \"设备号\" STRING ID, \"温度\" FLOAT MEASUREMENT, \"排量\" DOUBLE MEASUREMENT) with (TTL=" + ttl + ")");
            }
        }
        // 计算实际TTL数量
        try (SessionDataSet dataSet = session.executeQueryStatement("SHOW TABLES")) {
            while (dataSet.hasNext()) {
                if (dataSet.next().getFields().get(1) != null) {
                    actual++;
                }
            }
        }
        // 比较是否符合预期
        assert expect == actual : "TestTTL_V1_Normal的setTTL实际不一致期待：" + expect + "，实际：" + actual;
    }

    /**
     * 测试查看TTL
     */
    @Test(priority = 20) // 测试执行的优先级为10
    public void showTTL() throws IOException, IoTDBConnectionException, StatementExecutionException {
        ArrayList<String> showTTL = new ArrayList<>();
        ArrayList<String> createTTL = new ArrayList<>();
        // 计算实际TTL数量
        try (SessionDataSet dataSet = session.executeQueryStatement("SHOW TABLES")) {
            while (dataSet.hasNext()) {
                showTTL.add(String.valueOf(dataSet.next().getFields().get(1)));
            }
        }
        System.out.println("----------------------------");
        // 获取解析后的数据
        for (Iterator<Object[]> it = getData(); it.hasNext(); ) {
            // 获取每行数据
            Object[] ttls = it.next();
            // 获取该行每列的数据
            for (Object ttl : ttls) {
                createTTL.add((String) ttl);
            }
        }
        // 比较是否符合预期
        for (int i = 0; i < showTTL.size(); i++) {
            assert showTTL.get(i).equals(createTTL.get((showTTL.size() - 1) - i)) : "TestTTL_V1_Normal的showTTL实际TTL不一致";
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
