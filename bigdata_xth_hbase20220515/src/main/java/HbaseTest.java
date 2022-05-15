

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;


public class HbaseTest {


    public static void main(String[] args) throws IOException {

        // 第一步：建立连接

        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum","emr-header-1");
        configuration.set("hbase.zookeeper.property.clientPort", "2181");

        System.out.println("configuration Connection..." + configuration.get("hbase.master"));
        Connection conn = ConnectionFactory.createConnection(configuration);
        Admin admin = conn.getAdmin();
        System.out.println("Admin Connectioned");

        TableName tableName = TableName.valueOf("xutianhui:student");
        String colFamily1 = "info";
        String colFamily2 = "score";

        String rowKey = "xutianhui";



        // 第二步：建表
        if (admin.tableExists(tableName)) {
            System.out.println("Table already exists");
        } else {
            HTableDescriptor hTableDescriptor = new HTableDescriptor(tableName);
            HColumnDescriptor hColumnDescriptor1 = new HColumnDescriptor(colFamily1);
            hTableDescriptor.addFamily(hColumnDescriptor1);
            HColumnDescriptor hColumnDescriptor2 = new HColumnDescriptor(colFamily2);
            hTableDescriptor.addFamily(hColumnDescriptor2);

            admin.createTable(hTableDescriptor);
            System.out.println("Table create successful");
        }


        //第三步：插入数据

        Put put = new Put(Bytes.toBytes(rowKey)); // row key
        put.addColumn(Bytes.toBytes(colFamily1), Bytes.toBytes("student_id"), Bytes.toBytes("G20220735030009")); // cf1:col1
        put.addColumn(Bytes.toBytes(colFamily1), Bytes.toBytes("class"), Bytes.toBytes("2")); // cf1:col2


        //put.addColumn(Bytes.toBytes(colFamily), Bytes.toBytes("name"), Bytes.toBytes("Tom")); // col2
        conn.getTable(tableName).put(put);
        System.out.println("Data insert success");


        // 第四步：查看数据
        Get get = new Get(Bytes.toBytes(rowKey));
        if (!get.isCheckExistenceOnly()) {
            Result result = conn.getTable(tableName).get(get);
            for (Cell cell : result.rawCells()) {
                String colName = Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength());
                String value = Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
                System.out.println("Data get success, colName: " + colName + ", value: " + value);
            }
        }

        // 删除数据
        Delete delete = new Delete(Bytes.toBytes(rowKey));      // 指定rowKey
        conn.getTable(tableName).delete(delete);
        System.out.println("Delete Success");

        // 删除表
        if (admin.tableExists(tableName)) {
            admin.disableTable(tableName);
            admin.deleteTable(tableName);
            System.out.println("Table Delete Successful");
        } else {
            System.out.println("Table does not exist!");
        }




    }
}
