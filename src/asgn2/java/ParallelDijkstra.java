
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.BufferedReader;

import java.util.Map;
import java.util.UUID;

import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;

import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;


public class ParallelDijkstra {

  public enum COUNTER {
    TOTAL_NODES,
    FOUND_NODES,
    UPDATED_NODES
  }

  public static void main(String[] args) throws Exception {
    FileSystem fs = FileSystem.get(new Configuration());
    Path inFilePath = new Path(args[0]);
    Path outFilePath = new Path("/tmp/" + UUID.randomUUID().toString());
    Path outTextFilePath = new Path("/out/" + UUID.randomUUID().toString());
    long rootNode = Long.parseLong(args[1]);
    int maxIter = Integer.parseInt(args[2]);

    /* =============================================
     * Pre-Process Job
     */
    Configuration preConf = new Configuration();
    preConf.set("mapreduce.input.keyvaluelinerecordreader.key.value.separator", " ");
    preConf.setLong("paralleldijkstra.root.node", rootNode);

    Job preJob = Job.getInstance(preConf, "pre");
    preJob.setJarByClass(PDPreProcess.class);
    preJob.setInputFormatClass(KeyValueTextInputFormat.class);

    preJob.setMapperClass(PDPreProcess.Map.class);
    preJob.setMapOutputKeyClass(Text.class);
    preJob.setMapOutputValueClass(Text.class);

    preJob.setReducerClass(PDPreProcess.Reduce.class);
    preJob.setOutputKeyClass(LongWritable.class);
    preJob.setOutputValueClass(PDNodeWritable.class);
    preJob.setOutputFormatClass(SequenceFileOutputFormat.class);

    FileInputFormat.addInputPath(preJob, inFilePath);
    FileOutputFormat.setOutputPath(preJob, outFilePath);
    System.out.println("== Pre-Process =======================================");
    System.out.println("Input: " + inFilePath);
    System.out.println("Output: " + outFilePath);

    if(!preJob.waitForCompletion(true)) {
      System.exit(1);
    }

    /* ==============================================
     * Parallel Dijkstra Job
     */
    long totalNodes = preJob.getCounters().findCounter(COUNTER.TOTAL_NODES).getValue();
    long foundNodes = 1; // root node is found.
    long updatedNodes;
    int iter = 1;

    while(iter <= maxIter || maxIter == 0) {
      inFilePath = outFilePath;
      outFilePath = new Path("/tmp/" + UUID.randomUUID().toString());
      outTextFilePath = new Path("/out/" + UUID.randomUUID().toString());

      Configuration bfsConf = new Configuration();
      bfsConf.set("mapreduce.output.textoutputformat.separator", " ");
      bfsConf.set("paralleldijkstra.output.path", outTextFilePath.toString());

      Job bfsJob = Job.getInstance(bfsConf, "bfs");
      bfsJob.setJarByClass(ParallelDijkstra.class);
      bfsJob.setInputFormatClass(SequenceFileInputFormat.class);

      bfsJob.setMapperClass(ParallelDijkstra.Map.class);
      bfsJob.setMapOutputKeyClass(LongWritable.class);
      bfsJob.setMapOutputValueClass(PDNodeWritable.class);

      bfsJob.setReducerClass(ParallelDijkstra.Reduce.class);
      bfsJob.setOutputKeyClass(LongWritable.class);
      bfsJob.setOutputValueClass(PDNodeWritable.class);
      bfsJob.setOutputFormatClass(SequenceFileOutputFormat.class);
      MultipleOutputs.addNamedOutput(bfsJob, "text",
          TextOutputFormat.class,
          LongWritable.class,
          LongWritable.class);

      FileInputFormat.addInputPath(bfsJob, inFilePath);
      FileOutputFormat.setOutputPath(bfsJob, outFilePath);
      System.out.println("== BFS Depth: "+ iter +"/"+ maxIter +" =======================================");
      System.out.println("Input: " + inFilePath);
      System.out.println("Output: " + outFilePath);
      System.out.println("Text Out: " + outTextFilePath);

      if(!bfsJob.waitForCompletion(true)) {
        System.exit(1);
      }
      
      foundNodes += bfsJob.getCounters().findCounter(COUNTER.FOUND_NODES).getValue();
      updatedNodes = bfsJob.getCounters().findCounter(COUNTER.UPDATED_NODES).getValue();
      System.out.println("== RESULT ======================================= ");
      System.out.println("Nodes Updated: " + updatedNodes);
      System.out.println("Found " + foundNodes + "/" + totalNodes + " nodes\n");
      if(updatedNodes == 0) break;

      iter++;
    }

    /* ==============================================
     * Print Output
     */
    try(InputStream is = fs.open(new Path(outTextFilePath, "text-r-00000"));
        InputStreamReader isr = new InputStreamReader(is);
        BufferedReader br = new BufferedReader(isr)) {
      String line;
      while((line = br.readLine()) != null) {
        System.out.println(line);
      }
    } catch (Exception e) {
      e.printStackTrace();
    }

    System.exit(0);
  }

  public static class Map
      extends Mapper<LongWritable,PDNodeWritable,LongWritable,PDNodeWritable> {

      public void map(LongWritable id, PDNodeWritable node, Context ctx)
        throws IOException, InterruptedException {
        ctx.write(id, node);
        if(node.dist.get() >= 0) {
          for(java.util.Map.Entry<Writable,Writable> edge : node.adjList.entrySet()) {
            PDNodeWritable neighbour = new PDNodeWritable();
            LongWritable weight = (LongWritable) edge.getValue();
            neighbour.id = (LongWritable) edge.getKey();
            neighbour.dist.set(weight.get() + node.dist.get());
            ctx.write(neighbour.id, neighbour);
          }
        }
      }
  }

  public static class Reduce
      extends Reducer<LongWritable,PDNodeWritable,LongWritable,PDNodeWritable> {
      private MultipleOutputs<LongWritable,LongWritable> out;

      public void setup(Context ctx) {
        out = new MultipleOutputs(ctx);
      }

      public void reduce(LongWritable id, Iterable<PDNodeWritable> nodes, Context ctx)
        throws IOException, InterruptedException {
        String outTextFilePath = ctx.getConfiguration().get("paralleldijkstra.output.path");
        PDNodeWritable minNode = new PDNodeWritable();
        minNode.id = id;
        long prevWeight = -1;
        for(PDNodeWritable node : nodes) {
          if(!node.adjList.isEmpty()) {
            minNode.adjList.putAll(node.adjList);
            prevWeight = node.dist.get();
          }
          long d = node.dist.get();
          long md = minNode.dist.get();
          if(d >= 0 && (md < 0 || d < md)) {
            minNode.dist.set(d);
          }
        }
        ctx.write(id, minNode);
        if(minNode.dist.get() >= 0) {
          out.write("text", id, minNode.dist.get(), outTextFilePath+"/text");
          if(prevWeight < 0) {
            ctx.getCounter(COUNTER.FOUND_NODES).increment(1);
          }
          if(prevWeight != minNode.dist.get()) {
            ctx.getCounter(COUNTER.UPDATED_NODES).increment(1);
          }
        }
      }

      public void cleanup(Context ctx)
        throws IOException, InterruptedException {
         out.close();
      }
  }

}
