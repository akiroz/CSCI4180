
import java.io.IOException;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;

public class PDPreProcess {

  public static class Map
      extends Mapper<Text,Text,Text,Text> {
  }
 
  public static class Reduce
      extends Reducer<Text,Text,LongWritable,PDNodeWritable> {

      public LongWritable sToLW(String val) {
        return new LongWritable(Long.parseLong(val));
      }

      public void reduce(Text key, Iterable<Text> vals, Context ctx)
        throws IOException, InterruptedException {
        Long rootNode = ctx.getConfiguration().getLong("paralleldijkstra.root.node", 0);

        PDNodeWritable node = new PDNodeWritable();
        node.id = sToLW(key.toString());
        if(node.id.get() == rootNode) {
          node.dist.set(0L);
        }

        for(Text v : vals) {
          String[] nw = v.toString().split(" ");
          node.adjList.put(sToLW(nw[0]), sToLW(nw[1]));
        }

        ctx.write(node.id, node);
        ctx.getCounter(ParallelDijkstra.COUNTER.TOTAL_NODES).increment(1);
      }
  }

}
