package org.apache.iotdb.api.test;

import org.apache.iotdb.api.test.utils.GenerateValues;
import org.apache.iotdb.api.test.utils.PrepareConnection;
import org.apache.iotdb.api.test.utils.ReadConfig;
import org.apache.iotdb.isession.SessionDataSet;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.Session;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.read.common.RowRecord;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.jetbrains.annotations.NotNull;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.log4testng.Logger;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.StringJoiner;

/**
 * 基础测试套件
 */
public class BaseTestSuite_TableModel {
    // 日志记录器，用于记录日志信息
    public Logger logger = Logger.getLogger(BaseTestSuite_TableModel.class);
    // 表模型session会话
    public Session session = null;

    @BeforeClass
    public void beforeSuite() throws IoTDBConnectionException {
        // 获取表模型的session
        session = PrepareConnection.getSession_TableModel();
    }

    @AfterClass
    public void afterSuie() throws IoTDBConnectionException {
        // 关闭表模型的session
        session.close();
    }

}
