import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

@SuppressWarnings("deprecation")
public class TablePartF {

  public static void main(String[] args) throws IOException {
    Configuration config = HBaseConfiguration.create();
    try (HTable table = new HTable(config, "powers")) {
      Scan scan = new Scan();
      try (ResultScanner scanner1 = table.getScanner(scan);) {
        for (Result result1 = scanner1.next(); result1 != null; result1 = scanner1.next()) {
          String name = Bytes.toString(result1.getValue(Bytes.toBytes("professional"), Bytes.toBytes("name")));
          String power = Bytes.toString(result1.getValue(Bytes.toBytes("personal"), Bytes.toBytes("power")));
          String color = Bytes.toString(result1.getValue(Bytes.toBytes("custom"), Bytes.toBytes("color")));
          try (ResultScanner scanner2 = table.getScanner(scan)) {
            for (Result result2 = scanner2.next(); result2 != null; result2 = scanner2.next()) {

              String name1 = Bytes.toString(result2.getValue(Bytes.toBytes("professional"), Bytes.toBytes("name")));
              String power1 = Bytes.toString(result2.getValue(Bytes.toBytes("personal"), Bytes.toBytes("power")));
              String color1 = Bytes.toString(result2.getValue(Bytes.toBytes("custom"), Bytes.toBytes("color")));

              if (color.equals(color1) && !name.equals(name1)) {
                System.out.println(name + ", " + power + ", " + name1 + ", " + power1 + ", " + color);
              }
            }
          }
        }
      }
    }
  }
}
