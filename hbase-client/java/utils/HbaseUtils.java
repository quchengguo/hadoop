package com.study.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by quchengguo on 2018/7/10.
 * Hbase基本API的封装
 */
public class HbaseUtils {
    private static Connection connection;

    public static ArrayList<ArrayList<Map<String, String>>> scan(String tName, String startKey, String stopKey, Filter filter) throws IOException {
        Table table = getConnection().getTable(TableName.valueOf(tName));
        Scan scan = new Scan();
        if(startKey != null && stopKey != null){
            scan.setStartRow(Bytes.toBytes(startKey));
            scan.setStopRow(Bytes.toBytes(stopKey));
        }
        if(filter != null){
            scan.setFilter(filter);
        }
        Result result = null;
        ResultScanner scanner = table.getScanner(scan);
        ArrayList<ArrayList<Map<String, String>>> arrayLists = new ArrayList<ArrayList<Map<String, String>>>();
        while ((result = scanner.next()) != null) {
            ArrayList<Map<String, String>> value = getValue(result);
            arrayLists.add(value);
        }
        return arrayLists;
    }

    private static ArrayList<Map<String,String>> getValue(Result result) {
        ArrayList<Map<String, String>> maps = new ArrayList<Map<String, String>>();
        List<Cell> cells = result.listCells();
        for (Cell cell : cells) {
            HashMap<String, String> hashMap = new HashMap<String, String>();
            hashMap.put("RowKey", Bytes.toString(CellUtil.cloneRow(cell)));
            hashMap.put("Family", Bytes.toString(CellUtil.cloneFamily(cell)));
            hashMap.put("Field", Bytes.toString(CellUtil.cloneQualifier(cell)));
            hashMap.put("Value", Bytes.toString(CellUtil.cloneValue(cell)));
            maps.add(hashMap);
        }
        return maps;
    }

    /**
     * 得到一个put方法
     * 这个方法不支持大量数据的传入
     *
     * @param tName  表名
     * @param rowkey 行键值
     * @param cf     列簇名
     * @param filed  字段名，列名
     * @param value  列的值
     * @throws IOException
     */
    public static void put(String tName, String rowkey, String cf, String filed, String value) throws IOException {
        Table table = getConnection().getTable(TableName.valueOf(tName));
        Put put = new Put(Bytes.toBytes(rowkey));
        put.addColumn(Bytes.toBytes(cf), Bytes.toBytes(filed), Bytes.toBytes(value));
        table.put(put);
        table.close();
    }

    /**
     * 该api提供的创建表的方式，必须是表不存在。
     * 如果表存在，就需要hbase的管理员去通过hbase shell命令去删除。
     *
     * @throws IOException
     */
    public static void createTable(String tName, String... familyNames) throws IOException {
        Admin admin = getConnection().getAdmin();
        TableName tableName = TableName.valueOf(tName);
        if (!admin.tableExists(tableName)) {
            HTableDescriptor hTableDescriptor = new HTableDescriptor(tableName);
            for (String familyName : familyNames) {
                hTableDescriptor.addFamily(new HColumnDescriptor(familyName));
            }
            admin.createTable(hTableDescriptor);
        }
        admin.close();
    }

    /**
     * 查询方法
     * @param tName
     * @param rowkey
     * @param cf
     * @param field
     * @return
     * @throws IOException
     */
    public static ArrayList<Map<String, String>> get(String tName, String rowkey, String cf, String field) throws IOException {
        Table table = getConnection().getTable(TableName.valueOf(tName));
        Get get = new Get(Bytes.toBytes(rowkey));
        if (cf != null) {
            get.addFamily(Bytes.toBytes(cf));
        }
        if (field != null) {
            get.addColumn(Bytes.toBytes(cf), Bytes.toBytes(field));
        }
        Result result = table.get(get);
        return getValue( result);
    }


    /**
     * 获取hbase连接
     * @return
     */
    private static Connection getConnection() throws IOException {
        if(connection == null){
            Configuration config = HBaseConfiguration.create();
            Connection connection = ConnectionFactory.createConnection();
        }
        return connection;
    }
}
