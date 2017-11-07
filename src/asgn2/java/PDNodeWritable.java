
import java.io.IOException;
import java.io.DataInput;
import java.io.DataOutput;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;

public class PDNodeWritable implements Writable {
  public LongWritable id;
  public LongWritable dist;
  // LongWritable(node) -> LongWritable(weight)
  public MapWritable adjList;

  public PDNodeWritable() {
    id = new LongWritable();
    dist = new LongWritable(-1L);
    adjList = new MapWritable();
  }

  public void write(DataOutput out) throws IOException {
    id.write(out);
    dist.write(out);
    adjList.write(out);
  }

  public void readFields(DataInput in) throws IOException {
    id.readFields(in);
    dist.readFields(in);
    adjList.readFields(in);
  }
}
