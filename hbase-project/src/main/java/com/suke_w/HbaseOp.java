package com.suke_w;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

public class HbaseOp {
    private static final Logger logger = LoggerFactory.getLogger(HbaseOp.class);
    public static final String OP_ROW_KEY = "WangShuoKai";
    private static Connection connection = null;
    private static Admin admin = null;

    static {
        Configuration config = HBaseConfiguration.create();
        config.set("hbase.zookeeper.quorum", "emr-worker-2,emr-worker-1,emr-header-1");
        try {
            connection = ConnectionFactory.createConnection(config);
            admin = connection.getAdmin();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {

        String tableName = "wangshuokai:student";

        /** 创建表 */
        createTable(tableName,"info","score");

        /** 初始化数据 */
        HashMap<String, List<Long>> map = new HashMap<>();
        map.put("Tom", Arrays.asList(20210000000001L, 1L, 75L, 82L));
        map.put("Jerry", Arrays.asList(20210000000002L, 1L, 85L, 82L));
        map.put("ZhangSan", Arrays.asList(20210000000003L, 2L, 71L, 69L));
        map.put("LiSi", Arrays.asList(20210000000004L, 2L, 77L, 72L));
        map.put(OP_ROW_KEY, Arrays.asList(20210000000005L, 3L, 99L, 99L));

        /** 添加数据 */
        map.forEach((k, v) -> {
            try {
                putData(tableName, k, "info", "student_id", v.get(0).toString());
                putData(tableName, k, "info", "class", v.get(1).toString());
                putData(tableName, k, "score", "understanding", v.get(2).toString());
                putData(tableName, k, "score", "programming", v.get(3).toString());
            } catch (Exception e) {
                logger.error("puData Failed", e);
            }
        });

        /** 查询数据 */
        try {
        getData(tableName,OP_ROW_KEY,"info","student_id");
        getData(tableName,OP_ROW_KEY,"score",null);
        } catch (Exception e) {
            logger.error("getData Failed", e);
        }

        /** 删除数据 */
        try {
        deleteData(tableName,OP_ROW_KEY,"info","student_id");
        } catch (Exception e) {
            logger.error("getData Failed", e);
        }
    }

    /**
     * 建表
     *
     * @param tableName
     * @param columnFamilies
     * @return
     */
    public static boolean createTable(String tableName, String... columnFamilies) {
        if (StringUtils.isEmpty(tableName) || columnFamilies.length < 1) {
            throw new IllegalArgumentException("tableName or columnFamilies is null");
        }
        TableDescriptorBuilder tableDescriptorBuilder = TableDescriptorBuilder.newBuilder(TableName.valueOf(tableName));
        for (String columnFamily : columnFamilies) {
            ColumnFamilyDescriptor columnFamilyDescriptor = ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(columnFamily)).build();
            tableDescriptorBuilder.setColumnFamily(columnFamilyDescriptor);
        }
        try {
            admin.createTable(tableDescriptorBuilder.build());
            logger.info("createTable sucess，tablename：{}", tableName);
            return true;
        } catch (IOException e) {
            e.printStackTrace();
            logger.error("createTable failed，tableName：{}", tableName, e);
        }
        return false;
    }

    /**
     * 插入数据
     *
     * @param tableName
     * @param rowKey
     * @param colFamily
     * @param colKey
     * @param colValue
     * @throws IOException
     */
    public static void putData(String tableName, String rowKey, String colFamily, String colKey, String colValue) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tableName));
        Put put = new Put(Bytes.toBytes(rowKey));
        put.addColumn(Bytes.toBytes(colFamily), Bytes.toBytes(colKey), Bytes.toBytes(colValue));
        table.put(put);
        table.close();
    }

    /**
     * 随机查询
     *
     * @param tableName
     * @param rowKey
     * @param colFamily
     * @param colkey
     * @throws IOException
     */
    public static void getData(String tableName, String rowKey, String colFamily, String colkey) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tableName));
        Get get = new Get(Bytes.toBytes(rowKey));
        if (StringUtils.isEmpty(colkey)) {
            get.addFamily(Bytes.toBytes(colFamily));
        } else {
            get.addColumn(Bytes.toBytes(colFamily), Bytes.toBytes(colkey));
        }
        Result result = table.get(get);
        //Cell[] cells = result.rawCells();
        List<Cell> cells = result.listCells();
        for (Cell cell : cells) {
            String row = new String(CellUtil.cloneRow(cell));
            String family = new String(CellUtil.cloneFamily(cell));
            String qualifier = new String(CellUtil.cloneQualifier(cell));
            String value = new String(CellUtil.cloneValue(cell));
            logger.info("RowKey:{}, Falimy:{}, Qualifier:{}, Value:{}", row, family, qualifier, value);
        }
        table.close();
    }

    /** 删除数据 */
    public static void deleteData(String tableName, String rowKey, String colFamily, String colKey) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tableName));
        Delete delete = new Delete(Bytes.toBytes(rowKey));
        delete.addColumn(Bytes.toBytes(colFamily), Bytes.toBytes(colKey));
        table.delete(delete);
        table.close();
    }
}
