
import java.io.IOException;

import java.util.ArrayList;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;

public class PRPreProcess {

  public static class Map
      extends Mapper<Text,Text,Text,Text> {

      public void map(Text key, Text val, Context ctx)
        throws IOException, InterruptedException {
        
        String[] nw = val.toString().split(" ");
        ctx.write(new Text(nw[0]), new Text());
        ctx.write(key, val);
      }
  }
 
  public static class Reduce
      extends Reducer<Text,Text,LongWritable,PRNodeWritable> {

      public LongWritable sToLW(String val) {
        return new LongWritable(Long.parseLong(val));
      }

      public void reduce(Text key, Iterable<Text> vals, Context ctx)
        throws IOException, InterruptedException {

        PRNodeWritable node = new PRNodeWritable();
        node.id = sToLW(key.toString());

        for(Text v : vals) {
          if(v.getLength() > 0) {
            String[] nw = v.toString().split(" ");
            node.adjList.put(sToLW(nw[0]), sToLW(nw[1]));
          }
        }

        ctx.write(node.id, node);
        ctx.getCounter(PageRank.COUNTER.TOTAL_NODES).increment(1);
      }
  }

}
