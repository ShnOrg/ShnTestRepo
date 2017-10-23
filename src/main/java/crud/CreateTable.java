package crud;


import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

public class CreateTable  {
public static void main(String x[]) throws IOException {
  Configuration configuration = HBaseConfiguration.create();
  Connection connection = ConnectionFactory.createConnection(configuration);
  try {
    Admin admin = connection.getAdmin();
    HTableDescriptor hTableDescriptor = new HTableDescriptor(TableName.valueOf("census"));
    hTableDescriptor.addFamily(new HColumnDescriptor("personal"));
    hTableDescriptor.addFamily(new HColumnDescriptor("professional"));
    if (!admin.tableExists(hTableDescriptor.getTableName())){
      System.out.println("Creating ..");
      admin.createTable(hTableDescriptor);
      System.out.println("Created");
    }
    else {
      System.out.println("Table already exists");
    }
  }
  finally {
connection.close();
  }
}
}
