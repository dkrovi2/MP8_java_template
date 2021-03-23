import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;

@SuppressWarnings("deprecation")
public class TablePartA{

   public static void createPowersTable(final HBaseAdmin admin) throws IOException {
      // Instantiating table descriptor class
      HTableDescriptor tableDescriptor = new
        HTableDescriptor(TableName.valueOf("powers"));

      // Adding column families to table descriptor
      tableDescriptor.addFamily(new HColumnDescriptor("personal"));
      tableDescriptor.addFamily(new HColumnDescriptor("professional"));
      tableDescriptor.addFamily(new HColumnDescriptor("custom"));

      // Execute the table through admin
      admin.createTable(tableDescriptor);
   }

   public static void createFoodTable(final HBaseAdmin admin) throws IOException {
      // Instantiating table descriptor class
      HTableDescriptor tableDescriptor = new
        HTableDescriptor(TableName.valueOf("food"));

      // Adding column families to table descriptor
      tableDescriptor.addFamily(new HColumnDescriptor("nutrition"));
      tableDescriptor.addFamily(new HColumnDescriptor("taste"));

      // Execute the table through admin
      admin.createTable(tableDescriptor);
   }

   public static void main(String[] args) throws IOException {
      // Instantiating configuration class
      Configuration con = HBaseConfiguration.create();

      // Instantiating HbaseAdmin class
      try (HBaseAdmin admin = new HBaseAdmin(con)) {
         createPowersTable(admin);
         createFoodTable(admin);
      }
   }
}

