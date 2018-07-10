package com.study;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.ValueFilter;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.List;

/**
 * Created by quchengguo on 2018/7/10.
 * Java API操作Hbase
 */
public class HBaseProcess {
    public static void main(String[] args) throws IOException {
        // 1.创建一个hbase的connection
        Configuration config = HBaseConfiguration.create();
        Connection connection = ConnectionFactory.createConnection();
        // 2.创建一个表，需要管理员权限
        Admin admin = connection.getAdmin();
        TableName tableName = TableName.valueOf("user");
        if(!admin.tableExists(tableName)){
            // 表不存在
            System.out.println("---------开始新建表------------------");
            HTableDescriptor hTableDescriptor = new HTableDescriptor(tableName);
            // 指定簇名
            hTableDescriptor.addFamily(new HColumnDescriptor("base_info"));
            // 创建表
            admin.createTable(hTableDescriptor);
            System.out.println("---------创建表成功------------------");
        }

        // 3.对表添加数据
        System.out.println("----------添加数据-------------");
        // 3.1拿到这个表
        Table user = connection.getTable(TableName.valueOf("user"));
        // 3.2添加数据
        byte[] rowkey = Bytes.toBytes("rowkey_16");
        byte[] family = Bytes.toBytes("base_info");
        Put put = new Put(rowkey);

        byte[] nameField = Bytes.toBytes("username");
        byte[] nameValue = Bytes.toBytes("张三的歌");
        put.addColumn(family, nameField, nameValue);

        byte[] sexField = Bytes.toBytes("sex");
        byte[] sexValue = Bytes.toBytes("0");
        put.addColumn(family, sexField, sexValue);

        byte[] birField = Bytes.toBytes("birthday");
        byte[] birValue = Bytes.toBytes("2018-08-10");
        put.addColumn(family, birField, birValue);

        byte[] addrField = Bytes.toBytes("address");
        byte[] addrValue = Bytes.toBytes("北京");
        put.addColumn(family, addrField, addrValue);

        // 3.3将put对象给user表，执行添加操作
        user.put(put);
        user.close();
        System.out.println("----------添加数据成功-------------");
        System.out.println("----------GET获取表数据-------------");
        Table userTable = connection.getTable(TableName.valueOf("user"));
        byte[] getRowkey = Bytes.toBytes("rowkey_16");
        Get get = new Get(getRowkey);
        Result result = userTable.get(get);
        List<Cell> cells = result.listCells();
        for (Cell cell : cells) {
            // 列簇、列名、值、rowkey
            // 打印rowkey,family,qualifier,value
            System.out.println(Bytes.toString(CellUtil.cloneRow(cell))
                    + "==> " + Bytes.toString(CellUtil.cloneFamily(cell))
                    + "{" + Bytes.toString(CellUtil.cloneQualifier(cell))
                    + ":" + Bytes.toString(CellUtil.cloneValue(cell)) + "}");
        }
        userTable.close();
        System.out.println("-----------扫描全表的数据-------------");
        Scan scan = new Scan();
        scan.setStartRow(Bytes.toBytes("rowkey_10"));
        scan.setStopRow(Bytes.toBytes("rowkey_17"));
        Table user1 = connection.getTable(TableName.valueOf("user"));
        ResultScanner scanner = user1.getScanner(scan);
        Result result1 = null;
        while ((result1 = scanner.next())!=null){
            List<Cell> cells1 = result1.listCells();
            for (Cell cell : cells1) {
                // 列簇、列名、值、rowkey
                // 打印rowkey,family,qualifier,value
                System.out.println(Bytes.toString(CellUtil.cloneRow(cell))
                        + "==> " + Bytes.toString(CellUtil.cloneFamily(cell))
                        + "{" + Bytes.toString(CellUtil.cloneQualifier(cell))
                        + ":" + Bytes.toString(CellUtil.cloneValue(cell)) + "}");
            }
        }
        user1.close();

        // 查询一个表中，所有住在北京市的用户
        System.out.println("-----------------查询一个表中，所有住在北京市的用户--------------------");
        // 创建一个filter
        ValueFilter filter = new ValueFilter(CompareFilter.CompareOp.EQUAL, new BinaryComparator("北京市".getBytes()));
        Scan scan1 = new Scan();
        scan1.setFilter(filter);
        // 定义好过滤之后，就可以作用于表
        Table user2 = connection.getTable(TableName.valueOf("user"));
        ResultScanner scanner2 = user2.getScanner(scan1);
        Result result2 = null;
        while ((result2 = scanner2.next()) != null){
            List<Cell> cells2 = result2.listCells();
            for(Cell cell : cells2){
                System.out.println(Bytes.toString(CellUtil.cloneRow(cell))
                        + "==> " + Bytes.toString(CellUtil.cloneFamily(cell))
                        + "{" + Bytes.toString(CellUtil.cloneQualifier(cell))
                        + ":" + Bytes.toString(CellUtil.cloneValue(cell)) + "}");
            }
        }
    }
}
