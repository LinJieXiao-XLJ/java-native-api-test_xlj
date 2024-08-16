package org.apache.iotdb.api.test;

import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.NoValidValueException;
import org.apache.iotdb.rpc.RedirectException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.service.rpc.thrift.TSInsertRecordsReq;
import org.apache.iotdb.session.Session;
import org.apache.tsfile.enums.TSDataType;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Test {
    public static void main(String[] args) throws IoTDBConnectionException, StatementExecutionException {
        Session session = new Session.Builder()
                .host("127.0.0.1")
                .port(6667)
                .enableRedirection(false)
                .maxRetryCount(0)
                .build();
        session.open();

        List<String> deviceIds = new ArrayList<>();
        List<Long> times = new ArrayList<>();
        List<String> measurements1 = new ArrayList<>();
        List<String> measurements2 = new ArrayList<>();
        List<List<String>> measurementsList = new ArrayList<>();
        List<List<TSDataType>> typesList = new ArrayList<>();
        List<List<Object>> valuesLists = new ArrayList<>();
        List<TSDataType> types1 = new ArrayList<>();
        List<TSDataType> types2 = new ArrayList<>();
        List<Object> values1 = new ArrayList<>();
        List<Object> values2 = new ArrayList<>();

        deviceIds.add("root.db.a1");
        deviceIds.add("root.db.a2");
        deviceIds.add("root.db.a1");
        deviceIds.add("root.db.a2");


        times.add(1L);
        times.add(2L);
        times.add(1L);
        times.add(2L);


        measurements1.add("s1");
        measurements1.add("s2");
        measurements1.add("s3");
        measurements1.add("s4");
        measurements1.add("s5");
        measurements1.add("s6");
        measurements1.add("s7");
        measurements1.add("s8");
        measurements1.add("s9");
        measurements1.add("s10");
        measurements2.add("s1");
        measurements2.add("s2");
        measurements2.add("s3");
        measurements2.add("s4");
        measurements2.add("s5");
        measurements2.add("s6");
        measurements2.add("s7");
        measurements2.add("s8");
        measurements2.add("s9");
        measurements2.add("s10");
        measurementsList.add(measurements1);
        measurementsList.add(measurements2);
        measurementsList.add(measurements1);
        measurementsList.add(measurements2);


        types1.add(TSDataType.INT32);
        types1.add(TSDataType.BOOLEAN);
        types1.add(TSDataType.INT64);
        types1.add(TSDataType.INT64);
        types1.add(TSDataType.INT64);
        types1.add(TSDataType.INT64);
        types1.add(TSDataType.INT64);
        types1.add(TSDataType.INT64);
        types1.add(TSDataType.INT64);
        types1.add(TSDataType.INT64);
        types2.add(TSDataType.INT32);
        types2.add(TSDataType.BOOLEAN);
        types2.add(TSDataType.INT64);
        types2.add(TSDataType.INT64);
        types2.add(TSDataType.INT64);
        types2.add(TSDataType.INT64);
        types2.add(TSDataType.INT64);
        types2.add(TSDataType.INT64);
        types2.add(TSDataType.INT64);
        types2.add(TSDataType.INT64);
        typesList.add(types1);
        typesList.add(types2);
        typesList.add(types1);
        typesList.add(types2);

//
//        10,null,1,1,1.0,1.0,123abc!()$没问题！（）￥,123abc!()$没问题！（）￥,iotdb6,1,2024-07-25
//        7,true,null,1,1.0,1.0,,123abc!()$没问题！（）￥,iotdb6,1,2024-07-25
//        9,true,1,null,1.0,1.0,123abc!()$没问题！（）￥,,iotdb6,1,2024-07-25
//        6,true,1,1,null,1.0,123abc!()$没问题！（）￥,123abc!()$没问题！（）￥,,1,2024-07-25
//        8,true,1,1,1.0,null, 123abc!()$没问题！（）￥ , 123abc!()$没问题！（）￥ ,iotdb6,1,2024-07-25

        values1.add(1);
        values1.add(null);
        values1.add(2L);
        values1.add(3L);
        values1.add(4L);
        values1.add(5L);
        values1.add(6L);
        values1.add(7L);
        values1.add(8L);
        values1.add(9L);
        values2.add(1);
        values2.add(null);
        values2.add(2L);
        values2.add(3L);
        values2.add(4L);
        values2.add(5L);
        values2.add(6L);
        values2.add(7L);
        values2.add(8L);
        values2.add(9L);
        valuesLists.add(values1);
        valuesLists.add(values2);
        valuesLists.add(values1);
        valuesLists.add(values2);

        System.out.println(types1);
        System.out.println(measurements1);

        session.insertRecords(deviceIds, times, measurementsList, typesList, valuesLists);
        System.out.println("|------执行insertRecords之后------|");

        System.out.println(types1);
        System.out.println(measurements1);


//        List<TSDataType> types = new ArrayList<>();
//        List<String> measurements = new ArrayList<>();
//        List<Object> values1 = new ArrayList<>();
//
//        measurements.add("s1");
//        measurements.add("s2");
//        measurements.add("s3");

//        types.add(TSDataType.INT32);
//        types.add(TSDataType.BOOLEAN);
//        types.add(TSDataType.INT64);

//        values1.add(1);
//        values1.add(null);
//        values1.add(1L);
//        System.out.println(values1.size());
//        System.out.println(values1);
//        System.out.println(values1.get(0));
//        System.out.println(values1.get(1));
//        System.out.println(values1.get(2));

//        session.insertRecord("root.sg28", 11L, measurements,types, null);
//        session.insertRecord("root.sg28", 11L, measurements,types, 1, null, 1L);
//        session.insertRecord("root.sg28", 11L, measurements,types, values1);
    }

//    @org.testng.annotations.Test
//    public void test1() throws IoTDBConnectionException, StatementExecutionException {
//        List<String> deviceIds = new ArrayList<>();
//        List<Long> times = new ArrayList<>();
//        List<String> measurements = new ArrayList<>();
//        List<List<String>> measurementsList = new ArrayList<>();
//        List<List<TSDataType>> typesList = new ArrayList<>();
//        List<List<Object>> valuesLists = new ArrayList<>();
//        List<TSDataType> types = new ArrayList<>();
//        List<Object> values1 = new ArrayList<>();
//
//        deviceIds.add("root.db");
//        deviceIds.add("root.db");
//
//        times.add(1L);
//        times.add(2L);
//
//        measurements.add("s1");
//        measurements.add("s2");
//        measurements.add("s3");
//        measurementsList.add(measurements);
//        measurementsList.add(measurements);
//
//        types.add(TSDataType.INT32);
//        types.add(TSDataType.BOOLEAN);
//        types.add(TSDataType.INT64);
//        typesList.add(types);
//        typesList.add(types);
//
//        values1.add(1);
//        values1.add(null);
//        values1.add(2L);
//        valuesLists.add(values1);
//        valuesLists.add(values1);
//
////        System.out.println(values1.size());
////        System.out.println(values1);
////        System.out.println(values1.get(0));
////        System.out.println(values1.get(1));
////        System.out.println(values1.get(2));
//
//        System.out.println(types);
//        System.out.println(measurements);
//
//        insertRecords(deviceIds, times, measurementsList, typesList, valuesLists);
//        System.out.println("|------666执行insertRecords之后------|");
//
//        System.out.println(types);
//        System.out.println(measurements);
//    }
//
//    public void insertRecords(
//            List<String> deviceIds,
//            List<Long> times,
//            List<List<String>> measurementsList,
//            List<List<TSDataType>> typesList,
//            List<List<Object>> valuesList)
//            throws IoTDBConnectionException, StatementExecutionException {
//
//        System.out.println(valuesList.get(0));
//        System.out.println(measurementsList.get(0));
//        System.out.println(typesList.get(0));
//        valuesList.get(0).remove(1);
//        measurementsList.get(0).remove(1);
//        typesList.get(0).remove(1);
////        filterAndGenTSInsertRecordsReq(deviceIds, times, measurementsList, typesList, valuesList);
//
//
//    }
//
//    private void filterAndGenTSInsertRecordsReq(
//            List<String> deviceIds,
//            List<Long> times,
//            List<List<String>> measurementsList,
//            List<List<TSDataType>> typesList,
//            List<List<Object>> valuesList) {
//        if (true) {
//            measurementsList = changeToArrayListWithStringType(measurementsList);
//            valuesList = changeToArrayList(valuesList);
//            deviceIds = new ArrayList<>(deviceIds);
//            times = new ArrayList<>(times);
//            typesList = changeToArrayListWithTSDataType(typesList);
//            filterNullValueAndMeasurement(deviceIds, times, measurementsList, valuesList, typesList);
//        }
//    }
//
//    private List<List<String>> changeToArrayListWithStringType(List<List<String>> values) {
//        if (!(values instanceof ArrayList)) {
//            values = new ArrayList<>(values);
//        }
//        for (int i = 0; i < values.size(); i++) {
//            List<String> currentValue = values.get(i);
//            if (!(currentValue instanceof ArrayList)) {
//                values.set(i, new ArrayList<>(currentValue));
//            }
//        }
//        return values;
//    }
//
//    private List<List<Object>> changeToArrayList(List<List<Object>> values) {
//        if (!(values instanceof ArrayList)) {
//            values = new ArrayList<>(values);
//        }
//        for (int i = 0; i < values.size(); i++) {
//            List<Object> currentValue = values.get(i);
//            if (!(currentValue instanceof ArrayList)) {
//                values.set(i, new ArrayList<>(currentValue));
//            }
//        }
//        return values;
//    }
//
//    private List<List<TSDataType>> changeToArrayListWithTSDataType(List<List<TSDataType>> values) {
//        if (!(values instanceof ArrayList)) {
//            values = new ArrayList<>(values);
//        }
//        for (int i = 0; i < values.size(); i++) {
//            List<TSDataType> currentValue = values.get(i);
//            if (!(currentValue instanceof ArrayList)) {
//                values.set(i, new ArrayList<>(currentValue));
//            }
//        }
//        return values;
//    }
//
//    private void filterNullValueAndMeasurement(
//            List<String> deviceIds,
//            List<Long> times,
//            List<List<String>> measurementsList,
//            List<List<Object>> valuesList,
//            List<List<TSDataType>> typesList) {
//        for (int i = valuesList.size() - 1; i >= 0; i--) {
//            List<Object> values = valuesList.get(i);
//            List<String> measurements = measurementsList.get(i);
//            List<TSDataType> types = typesList.get(i);
//            boolean isAllValuesNull =
//                    filterNullValueAndMeasurement(deviceIds.get(i), measurements, types, values);
//            if (isAllValuesNull) {
//                valuesList.remove(i);
//                measurementsList.remove(i);
//                deviceIds.remove(i);
//                times.remove(i);
//                typesList.remove(i);
//            }
//        }
//    }
//
//    private boolean filterNullValueAndMeasurement(
//            String deviceId,
//            List<String> measurementsList,
//            List<TSDataType> types,
//            List<Object> valuesList) {
//        Map<String, Object> nullMap = new HashMap<>();
//        for (int i = valuesList.size() - 1; i >= 0; i--) {
//            if (valuesList.get(i) == null) {
//                nullMap.put(measurementsList.get(i), valuesList.get(i));
//                valuesList.remove(i);
//                measurementsList.remove(i);
//                types.remove(i);
//            }
//        }
//        if (valuesList.isEmpty()) {
//            return true;
//        } else {
//        }
//        return false;
//    }

//    @org.testng.annotations.Test
    public void test1() {
        String prefixTableName = "tableName";
        int num = 1;
        for (int i = 0; i < 5; i++) {
            String tableName = prefixTableName + num++;
            System.out.println(tableName);
            System.out.println("create table " + tableName +
                    " (" +
                    "\"地区\" STRING ID, " +
                    "\"厂号\" STRING ID, " +
                    "\"设备号\" STRING ID, " +
                    "\"温度\" FLOAT MEASUREMENT, " +
                    "\"排量\" DOUBLE MEASUREMENT" +
                    ")" +
                    " with " +
                    "TTL=" + 1);
        }
    }

    @org.testng.annotations.Test
    public void test2() {
        Matcher matcher = Pattern.compile("table\\s*(\\w+)\\s*\\(").matcher("create table abcdefg ( \"地区\" STRING ID,\"型号\" STRING ATTRIBUTE,\"温度\" FLOAT MEASUREMENT,\"排量\" DOUBLE MEASUREMENT) with (TTL=3600000)");
        assert matcher.find() : "未找到表名";
        System.out.println(matcher.group(1).replaceAll("\\s+", ""));
    }
}
