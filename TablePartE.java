import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;

@SuppressWarnings("deprecation")
public class TablePartE {

  public static void main(String[] args) throws IOException {
    Configuration config = HBaseConfiguration.create();
    HTable table = new HTable(config, "powers");
    Scan scan = new Scan();

    try (ResultScanner scanner = table.getScanner(scan)) {
      for (Result result = scanner.next(); result != null; result = scanner.next()) {
        System.out.println(result);
      }
    }
  }
}

