package org.apache.iotdb.api.test;

import org.apache.iotdb.api.test.utils.CustomDataProvider;
import org.apache.iotdb.isession.SessionDataSet;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.testng.annotations.Test;

import java.io.IOException;
import java.nio.charset.Charset;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * Title：测试表模型SQL语句
 * Describe：
 * Author：肖林捷
 */
public class TestTable extends BaseTestSuite_TableModel {
    @Test
    public void test1() throws IoTDBConnectionException, StatementExecutionException {
        session.executeNonQueryStatement("create database ln");
        System.out.println("创建成功");
        session.executeNonQueryStatement("use ln");
        System.out.println("执行成功");
        session.executeNonQueryStatement("create table ln.a1 ( \"地区\" STRING ID,\"厂号\" STRING ID,\"设备号\" STRING ID,\"温度\" FLOAT MEASUREMENT,\"排量\" DOUBLE MEASUREMENT) with(TTL=360000)");
        session.executeNonQueryStatement("create table a2 ( \"地区\" STRING ID,\"厂号\" STRING ID,\"设备号\" STRING ID,\"温度\" FLOAT MEASUREMENT,\"排量\" DOUBLE MEASUREMENT) with(TTL=0)");
        session.executeNonQueryStatement("create table a3 ( \"地区\" STRING ID,\"厂号\" STRING ID,\"设备号\" STRING ID,\"温度\" FLOAT MEASUREMENT,\"排量\" DOUBLE MEASUREMENT) with(TTL=-360000)");
        session.executeNonQueryStatement("create table a4 ( \"地区\" STRING ID,\"厂号\" STRING ID,\"设备号\" STRING ID,\"温度\" FLOAT MEASUREMENT,\"排量\" DOUBLE MEASUREMENT) with(TTL=9223372036854775807)");
        try (SessionDataSet dataSet = session.executeQueryStatement("SHOW TABLES")) {
            System.out.println(dataSet.getColumnNames());
            while (dataSet.hasNext()) {
                System.out.println(dataSet.next().getFields());
            }
        }
        session.executeNonQueryStatement("drop database ln");
    }

    @Test
    public void test2() throws IoTDBConnectionException, StatementExecutionException {
        try {
            session.executeNonQueryStatement("create database db1");
        } catch (Exception e) {
            session.executeNonQueryStatement("drop database db1");
            session.executeNonQueryStatement("create database db1");
        }
        session.executeNonQueryStatement("USE \"db1\"");
        session.executeNonQueryStatement("create table table2 (" +
                "device_id string id," +
                "attribute STRING ATTRIBUTE," +
                "boolean boolean MEASUREMENT," +
                "int32 int32 MEASUREMENT," +
                "int64 int64 MEASUREMENT," +
                "float float MEASUREMENT," +
                "double double MEASUREMENT," +
                "text text MEASUREMENT," +
                "string string MEASUREMENT," +
                "blob blob MEASUREMENT," +
                "timestamp01 timestamp MEASUREMENT," +
                "date date MEASUREMENT)");

        List<IMeasurementSchema> schemas = new ArrayList<>();
        schemas.add(new MeasurementSchema("device_id", TSDataType.STRING));
        schemas.add(new MeasurementSchema("attribute", TSDataType.STRING));
        schemas.add(new MeasurementSchema("boolean", TSDataType.BOOLEAN));
        schemas.add(new MeasurementSchema("int32", TSDataType.INT32));
        schemas.add(new MeasurementSchema("int64", TSDataType.INT64));
        schemas.add(new MeasurementSchema("float", TSDataType.FLOAT));
        schemas.add(new MeasurementSchema("double", TSDataType.DOUBLE));
        schemas.add(new MeasurementSchema("text", TSDataType.TEXT));
        schemas.add(new MeasurementSchema("string", TSDataType.STRING));
        schemas.add(new MeasurementSchema("blob", TSDataType.BLOB));
        schemas.add(new MeasurementSchema("timestamp", TSDataType.TIMESTAMP));
        schemas.add(new MeasurementSchema("date", TSDataType.DATE));
        final List<Tablet.ColumnType> columnTypes = Arrays.asList(
                Tablet.ColumnType.ID,
                Tablet.ColumnType.ATTRIBUTE,
                Tablet.ColumnType.MEASUREMENT,
                Tablet.ColumnType.MEASUREMENT,
                Tablet.ColumnType.MEASUREMENT,
                Tablet.ColumnType.MEASUREMENT,
                Tablet.ColumnType.MEASUREMENT,
                Tablet.ColumnType.MEASUREMENT,
                Tablet.ColumnType.MEASUREMENT,
                Tablet.ColumnType.MEASUREMENT,
                Tablet.ColumnType.MEASUREMENT,
                Tablet.ColumnType.MEASUREMENT);

        long timestamp = 0;
        Tablet tablet = new Tablet("table2", schemas, columnTypes, 10);

        for (long row = 0; row < 10; row++) {
            int rowIndex = tablet.rowSize++;
            tablet.addTimestamp(rowIndex, timestamp + row);
            tablet.addValue("device_id", rowIndex, "1");
            tablet.addValue("attribute", rowIndex, "1");
            tablet.addValue("boolean", rowIndex, true);
            tablet.addValue("int32", rowIndex, Integer.valueOf("1"));
            tablet.addValue("int64", rowIndex, Long.valueOf("1"));
            tablet.addValue("float", rowIndex, Float.valueOf("1.0"));
            tablet.addValue("double", rowIndex, Double.valueOf("1.0"));
            tablet.addValue("text", rowIndex, "true");
            tablet.addValue("string", rowIndex, "true");
            tablet.addValue("blob", rowIndex, new Binary("iotdb", Charset.defaultCharset()));
            tablet.addValue("timestamp", rowIndex, 1L);
            tablet.addValue("date", rowIndex, LocalDate.parse("2024-08-15"));
        }
        session.insertRelationalTablet(tablet, true);
        session.executeNonQueryStatement("drop database db1");
    }

    @Test
    public void test3() throws IoTDBConnectionException, StatementExecutionException, IOException {
        try {
            session.executeNonQueryStatement("create database db1");
        } catch (Exception e) {
            session.executeNonQueryStatement("drop database db1");
            session.executeNonQueryStatement("create database db1");
        }
        session.executeNonQueryStatement("USE \"db1\"");
        session.executeNonQueryStatement("create table table2 (" +
                "device_id string id," +
                "attribute STRING ATTRIBUTE," +
                "boolean boolean MEASUREMENT," +
                "int32 int32 MEASUREMENT," +
                "int64 int64 MEASUREMENT," +
                "float float MEASUREMENT," +
                "double double MEASUREMENT," +
                "text text MEASUREMENT," +
                "string string MEASUREMENT," +
                "blob blob MEASUREMENT," +
                "timestamp timestamp MEASUREMENT," +
                "date date MEASUREMENT)");

        List<IMeasurementSchema> schemas = new ArrayList<>();
        schemas.add(new MeasurementSchema("device_id", TSDataType.STRING));
        schemas.add(new MeasurementSchema("attribute", TSDataType.STRING));
        schemas.add(new MeasurementSchema("boolean", TSDataType.BOOLEAN));
        schemas.add(new MeasurementSchema("int32", TSDataType.INT32));
        schemas.add(new MeasurementSchema("int64", TSDataType.INT64));
        schemas.add(new MeasurementSchema("float", TSDataType.FLOAT));
        schemas.add(new MeasurementSchema("double", TSDataType.DOUBLE));
        schemas.add(new MeasurementSchema("text", TSDataType.TEXT));
        schemas.add(new MeasurementSchema("string", TSDataType.STRING));
        schemas.add(new MeasurementSchema("blob", TSDataType.BLOB));
        schemas.add(new MeasurementSchema("timestamp", TSDataType.TIMESTAMP));
        schemas.add(new MeasurementSchema("date", TSDataType.DATE));
        List<Tablet.ColumnType> columnTypes = Arrays.asList(
                Tablet.ColumnType.ID,
                Tablet.ColumnType.ATTRIBUTE,
                Tablet.ColumnType.MEASUREMENT,
                Tablet.ColumnType.MEASUREMENT,
                Tablet.ColumnType.MEASUREMENT,
                Tablet.ColumnType.MEASUREMENT,
                Tablet.ColumnType.MEASUREMENT,
                Tablet.ColumnType.MEASUREMENT,
                Tablet.ColumnType.MEASUREMENT,
                Tablet.ColumnType.MEASUREMENT,
                Tablet.ColumnType.MEASUREMENT,
                Tablet.ColumnType.MEASUREMENT);
        Tablet tablet = new Tablet("table2", schemas, columnTypes, 10);

        // 获取解析后的数据
        for (Iterator<Object[]> it = getData2(); it.hasNext(); ) {
            // 获取每行的SQL语句
            Object[] line = it.next();
            // 实例化有效行并切换行索引
            int rowIndex = tablet.rowSize++;
            // 添加时间戳
            tablet.addTimestamp(rowIndex, Long.valueOf((String) line[0]));
            // 获取每行每列的数据
            for (int i = 0; i < schemas.size(); i++) {
                // 根据数据类型添加值到tablet
                switch (schemas.get(i).getType()) {
                    case BOOLEAN:
                        tablet.addValue(schemas.get(i).getMeasurementId(), rowIndex,
                                line[i + 1] == null ? false : Boolean.valueOf((String) line[i + 1]));
                        break;
                    case INT32:
                        tablet.addValue(schemas.get(i).getMeasurementId(), rowIndex,
                                line[i + 1] == null ? 1 : Integer.valueOf((String) line[i + 1]));
                        break;
                    case INT64:
                    case TIMESTAMP:
                        tablet.addValue(schemas.get(i).getMeasurementId(), rowIndex,
                                line[i + 1] == null ? 1L : Long.valueOf((String) line[i + 1]));
                        break;
                    case FLOAT:
                        tablet.addValue(schemas.get(i).getMeasurementId(), rowIndex,
                                line[i + 1] == null ? 1.01f : Float.valueOf((String) line[i + 1]));
                        break;
                    case DOUBLE:
                        tablet.addValue(schemas.get(i).getMeasurementId(), rowIndex,
                                line[i + 1] == null ? 1.0 : Double.valueOf((String) line[i + 1]));
                        break;
                    case TEXT:
                    case STRING:
                        tablet.addValue(schemas.get(i).getMeasurementId(), rowIndex,
                                line[i + 1] == null ? "stringnull" : line[i + 1]);
                        break;
                    case BLOB:
                        tablet.addValue(schemas.get(i).getMeasurementId(), rowIndex,
                                line[i + 1] == null ? new Binary("iotdb", Charset.defaultCharset()) : new Binary((String) line[i + 1], Charset.defaultCharset()));
                        break;
                    case DATE:
                        tablet.addValue(schemas.get(i).getMeasurementId(), rowIndex,
                                line[i + 1] == null ? LocalDate.parse("2024-08-15") : LocalDate.parse((CharSequence) line[i + 1]));
                        break;
                }
            }
        }
        session.insertRelationalTablet(tablet);
        session.executeNonQueryStatement("drop database db1");
    }

    public Iterator<Object[]> getData2() throws IOException {
        return new CustomDataProvider().load_table("data/tableModel/insertRelationalTablet_normal.csv", false).getData();
    }
}
