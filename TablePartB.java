import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;

public class TablePartB {

  public static void main(String[] args) throws IOException {

    // Instantiating configuration class
    Configuration con = HBaseConfiguration.create();

    // Instantiating HbaseAdmin class
    try (HBaseAdmin admin = new HBaseAdmin(con)) {


      HTableDescriptor[] tableDescriptor = admin.listTables();
      for (int i = 0; i < tableDescriptor.length; i++) {
        System.out.println(tableDescriptor[i].getNameAsString());
      }

    }
  }
}

