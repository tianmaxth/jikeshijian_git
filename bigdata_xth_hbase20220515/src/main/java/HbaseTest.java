
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HbaseTest {
    private static final Logger logger = LoggerFactory.getLogger(HbaseTest.class);
    public static final String OP_ROW_KEY = "xutianhui";
    private static Connection connection = null;
    private static Admin admin = null;


    public HbaseTest() {
    }




    public static boolean isTableExist(String tableName) {
        try {
            return admin.tableExists(TableName.valueOf(tableName));
        } catch (IOException var2) {
            logger.error("isTableExist failed, tableName: {}", tableName, var2);
            return false;
        }
    }



    public static boolean createTable(String tableName, String... columnFamilies) {
        if (!StringUtils.isEmpty(tableName) && columnFamilies.length >= 1) {
            TableDescriptorBuilder tDescBuilder = TableDescriptorBuilder.newBuilder(TableName.valueOf(tableName));
            String[] var3 = columnFamilies;
            int var4 = columnFamilies.length;

            for(int var5 = 0; var5 < var4; ++var5) {
                String columnFamily = var3[var5];
                ColumnFamilyDescriptor descriptor = ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(columnFamily)).build();
                tDescBuilder.setColumnFamily(descriptor);
            }

            try {
                admin.createTable(tDescBuilder.build());
                logger.info("createTable success, tableName: {}", tableName);
                return true;
            } catch (IOException var8) {
                logger.error("createTable failed, tableName: {}", tableName, var8);
                return false;
            }
        } else {
            throw new IllegalArgumentException("tableName or columnFamilies is null");
        }
    }




    public static void deleteTable(String tableName) throws IOException {
        admin.disableTable(TableName.valueOf(tableName));
        admin.deleteTable(TableName.valueOf(tableName));
        logger.info("deleteTable success, tableName: {}", tableName);
    }




    public static void putData(String tableName, String rowKey, String colFamily, String colKey, String colValue) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tableName));
        Put put = new Put(Bytes.toBytes(rowKey));
        put.addColumn(Bytes.toBytes(colFamily), Bytes.toBytes(colKey), Bytes.toBytes(colValue));
        table.put(put);
        table.close();
    }




    public static void getData(String tableName, String rowKey, String colFamily, String colKey) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tableName));
        Get get = new Get(Bytes.toBytes(rowKey));
        if (StringUtils.isEmpty(colKey)) {
            get.addFamily(Bytes.toBytes(colFamily));
        } else {
            get.addColumn(Bytes.toBytes(colFamily), Bytes.toBytes(colKey));
        }

        Result result = table.get(get);
        Cell[] var7 = result.rawCells();
        int var8 = var7.length;

        for(int var9 = 0; var9 < var8; ++var9) {
            Cell cell = var7[var9];
            String family = Bytes.toString(CellUtil.cloneFamily(cell));
            String qualifier = Bytes.toString(CellUtil.cloneQualifier(cell));
            String value = Bytes.toString(CellUtil.cloneValue(cell));
            logger.info("Family:{}, Qualifier:{}, Value:{}", new Object[]{family, qualifier, value});
        }

        table.close();
    }




    public static void scanTable(String tableName) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tableName));
        Scan scan = new Scan();
        ResultScanner resultScanner = table.getScanner(scan);
        Iterator var4 = resultScanner.iterator();

        while(var4.hasNext()) {
            Result result = (Result)var4.next();
            Cell[] var6 = result.rawCells();
            int var7 = var6.length;

            for(int var8 = 0; var8 < var7; ++var8) {
                Cell cell = var6[var8];
                String row = Bytes.toString(CellUtil.cloneRow(cell));
                String family = Bytes.toString(CellUtil.cloneFamily(cell));
                String qualifier = Bytes.toString(CellUtil.cloneQualifier(cell));
                String value = Bytes.toString(CellUtil.cloneValue(cell));
                logger.info("Row:{}, Family:{}, Qualifier:{}, Value:{}", new Object[]{row, family, qualifier, value});
            }
        }

    }




    public static void deleteData(String tableName, String rowKey, String colFamily, String colKey) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tableName));
        Delete delete = new Delete(Bytes.toBytes(rowKey));
        delete.addColumn(Bytes.toBytes(colFamily), Bytes.toBytes(colKey));
        table.delete(delete);
    }




    public static void close() {
        try {
            if (admin != null) {
                admin.close();
            }

            if (connection != null) {
                connection.close();
            }
        } catch (IOException var1) {
            logger.warn("close connection failed", var1);
        }

    }

    public static void main(String[] args) throws Exception {
        String tableName = "xutianhui:student";
        if (isTableExist(tableName)) {
            deleteTable(tableName);
        }

        createTable(tableName, "info", "score");



        Map<String, List<Long>> dataMap = new HashMap();
        dataMap.put("Tom", Arrays.asList(20210000000001L, 1L, 75L, 82L));
        dataMap.put("Jerry", Arrays.asList(20210000000002L, 1L, 85L, 67L));
        dataMap.put("Jack", Arrays.asList(20210000000003L, 2L, 80L, 80L));
        dataMap.put("Rose", Arrays.asList(20210000000004L, 2L, 60L, 61L));
        dataMap.put("xutianhui", Arrays.asList(20220735030009L, 3L, 90L, 90L));



        logger.info("put all data");
        dataMap.forEach((k, v) -> {
            try {
                putData(tableName, k, "info", "student_id", ((Long)v.get(0)).toString());
                putData(tableName, k, "info", "class", ((Long)v.get(1)).toString());
                putData(tableName, k, "score", "understanding", ((Long)v.get(2)).toString());
                putData(tableName, k, "score", "programming", ((Long)v.get(3)).toString());
            } catch (Exception var4) {
                logger.error("putData failed", var4);
            }

        });


        logger.info("get data");
        getData(tableName, "xutianhui", "info", "student_id");
        getData(tableName, "xutianhui", "score", (String)null);


        logger.info("scan table");
        scanTable(tableName);


        logger.info("delete data");
        deleteData(tableName, "xutianhui", "info", "student_id");
        deleteData(tableName, "xutianhui", "score", "programming");


        logger.info("get data");
        getData(tableName, "xutianhui", "info", (String)null);
        getData(tableName, "xutianhui", "score", (String)null);


        close();
    }

    static {
        try {
            Configuration configuration = HBaseConfiguration.create();
            configuration.set("hbase.zookeeper.quorum", "emr-worker-2,emr-worker-1,emr-header-1");
            configuration.set("hbase.zookeeper.property.clientPort", "2181");
            connection = ConnectionFactory.createConnection(configuration);
            admin = connection.getAdmin();
        } catch (IOException var1) {
            logger.error("init failed", var1);
        }

    }
}
