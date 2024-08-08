package org.apache.iotdb.api.test.tablemodel.database_manage;

import org.apache.iotdb.api.test.BaseTestSuite;
import org.apache.iotdb.api.test.utils.CustomDataProvider;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.testng.annotations.AfterClass;
import org.testng.annotations.DataProvider;

import java.io.IOException;
import java.util.Iterator;

/**
 * Title：测试数据库操作
 * Describe：基于表模型V1版本，测试数据库所有的操作，包括创建数据库、查看数据库、使用数据库和删除数据库
 * Author：肖林捷
 * Date：2024/8/8
 */
public class TestDataBase_V1 extends BaseTestSuite {

    /**
     * 获取正确的数据
     *
     * @return 对应文件的数据
     */
    @DataProvider(name = "insertSingleNormal")
    public Iterator<Object[]> getSingleNormal() throws IOException {
        return new CustomDataProvider().load("data/database.csv").getData();
    }

    /**
     * 在测试类之后执行的删除数据库
     */
    @AfterClass
    public void afterClass() throws IoTDBConnectionException, StatementExecutionException {
        // 删除数据库
    }

}
