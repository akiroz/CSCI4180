
import java.io.IOException;
import java.io.DataInput;
import java.io.DataOutput;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.MapWritable;

public class PRNodeWritable implements Writable {
  public LongWritable id;
  public DoubleWritable rank;
  // LongWritable(node) -> LongWritable(1)
  public MapWritable adjList;

  public PRNodeWritable() {
    id = new LongWritable();
    rank = new DoubleWritable(-1);
    adjList = new MapWritable();
  }

  public void write(DataOutput out) throws IOException {
    id.write(out);
    rank.write(out);
    adjList.write(out);
  }

  public void readFields(DataInput in) throws IOException {
    id.readFields(in);
    rank.readFields(in);
    adjList.readFields(in);
  }
}
