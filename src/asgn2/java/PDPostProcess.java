
import java.io.IOException;

import java.util.Arrays;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;

public class PDPostProcess {

  public static class Map
      extends Mapper<LongWritable,PDNodeWritable,Text,Text> {

      public void map(LongWritable id, PDNodeWritable node, Context ctx)
        throws IOException, InterruptedException {
        // DEBUG OUTPUT =====
        //ctx.write(
        //    new Text(id.toString()),
        //    new Text(
        //      node.dist.get() + " " +
        //      Arrays.toString(node.adjList.entrySet().toArray())
        //      )
        //    );
        if(node.dist.get() >= 0) {
          ctx.write(new Text(id.toString()), new Text(node.dist.toString()));
        }
      }
  }

  public static class Reduce
      extends Reducer<Text,Text,Text,Text> {
  }

}
