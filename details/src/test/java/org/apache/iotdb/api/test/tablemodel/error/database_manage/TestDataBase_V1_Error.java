package org.apache.iotdb.api.test.tablemodel.error.database_manage;

import org.apache.iotdb.api.test.BaseTestSuite_TableModel;
import org.apache.iotdb.api.test.utils.CustomDataProvider;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.Iterator;

/**
 * Title：测试创建数据库——异常情况
 * Describe：基于表模型V1版本，测试数据库所有的操作，包括创建数据库、查看数据库、使用数据库和删除数据库的异常情况
 * Author：肖林捷
 * Date：2024/8/8
 */
public class TestDataBase_V1_Error extends BaseTestSuite_TableModel {

    /**
     * 获取不正确的数据并解析文档
     */
    @DataProvider(name = "database")
    public Iterator<Object[]> getData() throws IOException {
        return new CustomDataProvider().load_table("data/tableModel/database_error.csv",false).getData();
    }

    /**
     * 测试创建非法命名的数据库
     */
    @Test(priority = 10)
    public void createDataBase() throws IOException {
        // 获取解析后的数据
        for (Iterator<Object[]> it = getData(); it.hasNext(); ) {
            // 获取每行数据
            Object[] dataBaseIds = it.next();
            // 获取每行每列的数据
            for (Object dataBaseId : dataBaseIds) {
                try {
                    // 创建数据库
                    session.executeNonQueryStatement("create database " + dataBaseId);
                } catch (Exception e) {
                    // 若不符合预期则断言
                    assert "org.apache.iotdb.rpc.StatementExecutionException".equals(e.getClass().getName()) : "TestDataBase_V1_Error的createDataBase 测试失败-其他错误:" + e;
                }
            }
        }
    }

    /**
     * 测试语法错误查看数据库
     */
    @Test(priority = 20)
    public void showDataBase() {
        try {
            // 查看数据库
            session.executeNonQueryStatement("showdatabases");
        } catch (Exception e) {
            // 若不符合预期则断言
            assert "org.apache.iotdb.rpc.StatementExecutionException".equals(e.getClass().getName()) : "TestDataBase_V1_Error的showDataBase 测试失败-其他错误:" + e;
        }

    }

    /**
     * 测试使用不存在的数据库
     */
    @Test(priority = 30)
    public void useDataBase() throws IOException {
        // 获取解析后的数据
        for (Iterator<Object[]> it = getData(); it.hasNext(); ) {
            // 获取每行数据
            Object[] dataBaseIds = it.next();
            // 获取每行每列的数据
            for (Object dataBaseId : dataBaseIds) {
                try {
                    // 使用数据库
                    session.executeNonQueryStatement("use " + dataBaseId);
                } catch (Exception e) {
                    // 若不符合预期则断言
                    assert "org.apache.iotdb.rpc.StatementExecutionException".equals(e.getClass().getName()) : "TestDataBase_V1_Error的useDataBase 测试失败-其他错误:" + e;
                }
            }
        }
    }

    /**
     * 测试删除不能存在的数据库
     */
    @Test(priority = 40) // 测试执行的优先级为10
    public void dropDataBase() {
        try {
            // 删除数据库
            session.executeNonQueryStatement("drop database nononono");
        } catch (Exception e) {
            // 若不符合预期则断言
            assert "org.apache.iotdb.rpc.StatementExecutionException".equals(e.getClass().getName()) && "500: Database nononono doesn't exist".equals(e.getMessage()) : "TestDataBase_V1_Error的dropDataBase 测试失败-其他错误:" + e;
        }
    }

}
