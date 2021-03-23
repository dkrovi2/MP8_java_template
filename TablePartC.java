import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

@SuppressWarnings("deprecation")
public class TablePartC {

  public static void main(String[] args) throws IOException {

    Configuration config = HBaseConfiguration.create();
    try (HTable hTable = new HTable(config, "powers")) {
      try (BufferedReader br = new BufferedReader(new FileReader("input.csv"))) {
        String row;
        while ((row = br.readLine()) != null) {
          String[] cols = row.trim().split(",");
          String rowKey = cols[0];
          String hero = cols[1];
          String power = cols[2];
          String name = cols[3];
          String xp = cols[4];
          String color = cols[5];

          Put p = new Put(Bytes.toBytes(rowKey));
          p.add(Bytes.toBytes("personal"),
            Bytes.toBytes("hero"), Bytes.toBytes(hero));
          p.add(Bytes.toBytes("personal"),
            Bytes.toBytes("power"), Bytes.toBytes(power));

          p.add(Bytes.toBytes("professional"),
            Bytes.toBytes("name"), Bytes.toBytes(name));
          p.add(Bytes.toBytes("professional"),
            Bytes.toBytes("xp"), Bytes.toBytes(xp));

          p.add(Bytes.toBytes("custom"),
            Bytes.toBytes("color"), Bytes.toBytes(color));
          hTable.put(p);
        }
      }
    }
  }
}

