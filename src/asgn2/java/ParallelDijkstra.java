
import java.io.IOException;

import java.util.Map;
import java.util.UUID;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

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


public class ParallelDijkstra {

  // Keep track for update
  public enum UpdateCounter {
    UPDATED
  }

  public static void main(String[] args) throws Exception {


    Path inFilePath = new Path(args[0]);
    Path outFilePath = new Path(args[1]);
    Path tmpFilePath;
    long rootNode = Long.parseLong(args[2]);
    int maxIter = Integer.parseInt(args[3]);
    int iter = 1;



    /* =============================================
     * Pre-Process Job
     */
    Configuration preConf = new Configuration();
    preConf.set("mapreduce.input.keyvaluelinerecordreader.key.value.separator", " ");
    preConf.setLong("paralleldijkstra.root.node", rootNode);

    Job preJob = Job.getInstance(preConf, "pre");
    preJob.setJarByClass(PDPreProcess.class);
    preJob.setInputFormatClass(KeyValueTextInputFormat.class);
    preJob.setOutputFormatClass(SequenceFileOutputFormat.class);

    preJob.setMapperClass(PDPreProcess.Map.class);
    preJob.setMapOutputKeyClass(Text.class);
    preJob.setMapOutputValueClass(Text.class);

    preJob.setReducerClass(PDPreProcess.Reduce.class);
    preJob.setOutputKeyClass(LongWritable.class);
    preJob.setOutputValueClass(PDNodeWritable.class);

    tmpFilePath = new Path("/tmp/" + UUID.randomUUID().toString());
    FileInputFormat.addInputPath(preJob, inFilePath);
    FileOutputFormat.setOutputPath(preJob, tmpFilePath);
    System.out.println("== Pre-Process =======================================");
    System.out.println("Input: " + inFilePath);
    System.out.println("Output: " + tmpFilePath);
    if(!preJob.waitForCompletion(true)) {
      System.exit(1);
    }

    /* ==============================================
     * Parallel Dijkstra Job
     */

    // Number of iterations specified
    if(maxIter!=0){
      while(iter <= maxIter) {
        Configuration bfsConf = new Configuration();

        Job bfsJob = Job.getInstance(bfsConf, "bfs");
        bfsJob.setJarByClass(ParallelDijkstra.class);
        bfsJob.setInputFormatClass(SequenceFileInputFormat.class);
        bfsJob.setOutputFormatClass(SequenceFileOutputFormat.class);

        bfsJob.setMapperClass(ParallelDijkstra.Map.class);
        bfsJob.setReducerClass(ParallelDijkstra.Reduce.class);

        bfsJob.setOutputKeyClass(LongWritable.class);
        bfsJob.setOutputValueClass(PDNodeWritable.class);

        System.out.println("== BFS Depth: "+ iter +" =======================================");
        FileInputFormat.addInputPath(bfsJob, tmpFilePath);
        System.out.println("Input: " + tmpFilePath);
        tmpFilePath = new Path("/tmp/" + UUID.randomUUID().toString());
        FileOutputFormat.setOutputPath(bfsJob, tmpFilePath);
        System.out.println("Output: " + tmpFilePath);
        if(!bfsJob.waitForCompletion(true)) {
          System.exit(1);
        }

        iter++;
      }
    }else{
      // counter
      long pre_count=-1;
      long count = 0;

      // Stop when there is no more updates in reduce tasks
      while(pre_count!=count){

        // same job config
        Configuration bfsConf = new Configuration();

        Job bfsJob = Job.getInstance(bfsConf, "bfs");


        pre_count=count;
        // debug check
        System.out.println("******************counter value = "+count);

        bfsJob.setJarByClass(ParallelDijkstra.class);
        bfsJob.setInputFormatClass(SequenceFileInputFormat.class);
        bfsJob.setOutputFormatClass(SequenceFileOutputFormat.class);

        bfsJob.setMapperClass(ParallelDijkstra.Map.class);
        bfsJob.setReducerClass(ParallelDijkstra.Reduce.class);

        bfsJob.setOutputKeyClass(LongWritable.class);
        bfsJob.setOutputValueClass(PDNodeWritable.class);

        System.out.println("== BFS Depth: "+ iter +" =======================================");
        FileInputFormat.addInputPath(bfsJob, tmpFilePath);
        System.out.println("Input: " + tmpFilePath);
        tmpFilePath = new Path("/tmp/" + UUID.randomUUID().toString());
        FileOutputFormat.setOutputPath(bfsJob, tmpFilePath);
        System.out.println("Output: " + tmpFilePath);
        if(!bfsJob.waitForCompletion(true)) {
          System.exit(1);
        }
        count=bfsJob.getCounters().findCounter(ParallelDijkstra.UpdateCounter.UPDATED)
        .getValue();
        System.out.println("New update count = "+count+" ///////// Previous update count = "+pre_count);
        iter++;
      }
    }

    /* ===============================================
     * Post-Process Job
     */
    Configuration postConf = new Configuration();
    postConf.set("mapreduce.output.textoutputformat.separator", " ");

    Job postJob = Job.getInstance(postConf, "post");
    postJob.setJarByClass(PDPostProcess.class);
    postJob.setInputFormatClass(SequenceFileInputFormat.class);
    postJob.setOutputFormatClass(TextOutputFormat.class);

    postJob.setMapperClass(PDPostProcess.Map.class);
    postJob.setReducerClass(PDPostProcess.Reduce.class);

    postJob.setOutputKeyClass(Text.class);
    postJob.setOutputValueClass(Text.class);

    FileInputFormat.addInputPath(postJob, tmpFilePath);
    FileOutputFormat.setOutputPath(postJob, outFilePath);
    System.out.println("== Post-Process ======================================");
    System.out.println("Input: " + tmpFilePath);
    System.out.println("Output: " + outFilePath);
    if(!postJob.waitForCompletion(true)) {
      System.exit(1);
    }

    /* ================================================
     * Done!
     */
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

      public void reduce(LongWritable id, Iterable<PDNodeWritable> nodes, Context ctx)
        throws IOException, InterruptedException {
        PDNodeWritable minNode = new PDNodeWritable();
        minNode.id = id;
        for(PDNodeWritable node : nodes) {
          if(!node.adjList.isEmpty()) {
            minNode.adjList.putAll(node.adjList);
          }
          long d = node.dist.get();
          long md = minNode.dist.get();
          if(d >= 0 && (md < 0 || d < md)) {
            minNode.dist.set(d);
            // There is update on the distance, increase counter by 1
            ctx.getCounter(UpdateCounter.UPDATED).increment(1);
          }
        }
        ctx.write(id, minNode);
      }
  }

}
