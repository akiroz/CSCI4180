
import java.io.IOException;
import java.io.DataInput;
import java.io.DataOutput;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.BooleanWritable;

public class PDNodeWritable implements Writable {
  public LongWritable id;
  public LongWritable dist;
  // LongWritable(node) -> LongWritable(weight)
  public MapWritable adjList;
  public BooleanWritable isPrev;

  public PDNodeWritable() {
    id = new LongWritable();
    dist = new LongWritable(-1);
    adjList = new MapWritable();
    isPrev = new BooleanWritable(false);
  }

  public void write(DataOutput out) throws IOException {
    id.write(out);
    dist.write(out);
    adjList.write(out);
    isPrev.write(out);
  }

  public void readFields(DataInput in) throws IOException {
    id.readFields(in);
    dist.readFields(in);
    adjList.readFields(in);
    isPrev.readFields(in);
  }
}
