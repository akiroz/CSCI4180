
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.DoubleWritable;

public class PRAdjust {

  public static class Map
      extends Mapper<LongWritable,PRNodeWritable,LongWritable,PRNodeWritable> {

      private MultipleOutputs<LongWritable,DoubleWritable> out;

      public void setup(Context ctx) {
        out = new MultipleOutputs(ctx);
      }

      public void map(LongWritable id, PRNodeWritable node, Context ctx)
        throws IOException, InterruptedException {
        Configuration cfg = ctx.getConfiguration();
        String outTextFilePath = cfg.get("pagerank.output.path");
        Double jumpFactor = cfg.getDouble("pagerank.jump.factor", 0);
        Double outThreshold = cfg.getDouble("pagerank.output.threshold", 0);
        Long missingMass = cfg.getLong("pagerank.missing.mass", 0);
        Long totalNodes = cfg.getLong("pagerank.total.nodes", 1);

        long rank = node.rank.get();
        rank += missingMass / totalNodes;
        rank *= 1 - jumpFactor;
        rank += jumpFactor * (PageRank.RANK_PRECISION / totalNodes);
        node.rank.set(rank);

        ctx.write(id, node);

        double outRank = ((double) node.rank.get()) / PageRank.RANK_PRECISION;
        if(outRank >= outThreshold) {
          out.write("text", id, new DoubleWritable(outRank), outTextFilePath+"/text");
        }
      }

      public void cleanup(Context ctx)
        throws IOException, InterruptedException {
         out.close();
      }
  }

  public static class Reduce
      extends Reducer<LongWritable,PRNodeWritable,LongWritable,PRNodeWritable> {
  }

}
