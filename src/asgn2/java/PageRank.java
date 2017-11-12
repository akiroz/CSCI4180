
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
import org.apache.hadoop.io.DoubleWritable;
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


public class PageRank {
  public static final long RANK_PRECISION = 1_000_000_000;

  public enum COUNTER {
    TOTAL_NODES,
    MISSING_MASS
  }

  public static void main(String[] args) throws Exception {
    FileSystem fs = FileSystem.get(new Configuration());
    Path inFilePath = new Path(args[0]);
    Path outFilePath = new Path("/tmp/" + UUID.randomUUID().toString());
    Path outTextFilePath = new Path("/out/" + UUID.randomUUID().toString());
    double jumpFactor = Double.parseDouble(args[1]);
    double outThreshold = Double.parseDouble(args[2]);
    int maxIter = Integer.parseInt(args[3]);

    /* =============================================
     * Pre-Process Job
     */
    Configuration preConf = new Configuration();
    preConf.set("mapreduce.input.keyvaluelinerecordreader.key.value.separator", " ");

    Job preJob = Job.getInstance(preConf, "pre");
    preJob.setJarByClass(PRPreProcess.class);
    preJob.setInputFormatClass(KeyValueTextInputFormat.class);

    preJob.setMapperClass(PRPreProcess.Map.class);
    preJob.setMapOutputKeyClass(Text.class);
    preJob.setMapOutputValueClass(Text.class);

    preJob.setReducerClass(PRPreProcess.Reduce.class);
    preJob.setOutputKeyClass(LongWritable.class);
    preJob.setOutputValueClass(PRNodeWritable.class);
    preJob.setOutputFormatClass(SequenceFileOutputFormat.class);

    FileInputFormat.addInputPath(preJob, inFilePath);
    FileOutputFormat.setOutputPath(preJob, outFilePath);
    System.out.println("== Pre-Process =======================================");
    System.out.println("Input: " + inFilePath);
    System.out.println("Output: " + outFilePath);
    System.out.println("\n");

    if(!preJob.waitForCompletion(true)) {
      System.exit(1);
    }

    /* ==============================================
     * Page Rank Job
     */
    long totalNodes = preJob.getCounters().findCounter(COUNTER.TOTAL_NODES).getValue();
    long missingMass = 0;
    int iter = 1;

    while(iter <= maxIter) {
      inFilePath = outFilePath;
      outFilePath = new Path("/tmp/" + UUID.randomUUID().toString());

      Configuration rankConf = new Configuration();
      rankConf.setDouble("pagerank.jump.factor", jumpFactor);
      rankConf.setLong("pagerank.total.nodes", totalNodes);

      Job rankJob = Job.getInstance(rankConf, "rank");
      rankJob.setJarByClass(PageRank.class);
      rankJob.setInputFormatClass(SequenceFileInputFormat.class);

      rankJob.setMapperClass(PageRank.Map.class);
      rankJob.setMapOutputKeyClass(LongWritable.class);
      rankJob.setMapOutputValueClass(PRNodeWritable.class);

      rankJob.setReducerClass(PageRank.Reduce.class);
      rankJob.setOutputKeyClass(LongWritable.class);
      rankJob.setOutputValueClass(PRNodeWritable.class);
      rankJob.setOutputFormatClass(SequenceFileOutputFormat.class);

      FileInputFormat.addInputPath(rankJob, inFilePath);
      FileOutputFormat.setOutputPath(rankJob, outFilePath);
      System.out.println("== Page Rank Iteration: "+ iter +"/"+ maxIter +" =======================================");
      System.out.println("Input: " + inFilePath);
      System.out.println("Output: " + outFilePath);
      System.out.println("\n");

      if(!rankJob.waitForCompletion(true)) {
        System.exit(1);
      }

      /* ==============================================
       * Redistribute Rank Mass
       */
      missingMass = rankJob.getCounters().findCounter(COUNTER.MISSING_MASS).getValue();
      inFilePath = outFilePath;
      outFilePath = new Path("/tmp/" + UUID.randomUUID().toString());
      outTextFilePath = new Path("/out/" + UUID.randomUUID().toString());

      Configuration adjustConf = new Configuration();
      adjustConf.set("mapreduce.output.textoutputformat.separator", " ");
      adjustConf.set("pagerank.output.path", outTextFilePath.toString());
      adjustConf.setDouble("pagerank.output.threshold", outThreshold);
      adjustConf.setDouble("pagerank.jump.factor", jumpFactor);
      adjustConf.setLong("pagerank.missing.mass", missingMass);
      adjustConf.setLong("pagerank.total.nodes", totalNodes);
      
      Job adjustJob = Job.getInstance(adjustConf, "adjust");
      adjustJob.setJarByClass(PRAdjust.class);
      adjustJob.setInputFormatClass(SequenceFileInputFormat.class);

      adjustJob.setMapperClass(PRAdjust.Map.class);
      adjustJob.setMapOutputKeyClass(LongWritable.class);
      adjustJob.setMapOutputValueClass(PRNodeWritable.class);

      adjustJob.setReducerClass(PRAdjust.Reduce.class);
      adjustJob.setOutputKeyClass(LongWritable.class);
      adjustJob.setOutputValueClass(PRNodeWritable.class);
      adjustJob.setOutputFormatClass(SequenceFileOutputFormat.class);
      MultipleOutputs.addNamedOutput(adjustJob, "text",
          TextOutputFormat.class,
          LongWritable.class,
          DoubleWritable.class);

      FileInputFormat.addInputPath(adjustJob, inFilePath);
      FileOutputFormat.setOutputPath(adjustJob, outFilePath);
      System.out.println("== Page Rank Adjust: "+ iter +"/"+ maxIter +" =======================================");
      System.out.println("Redist Mass: " + ((double) missingMass) / RANK_PRECISION);
      System.out.println("Input: " + inFilePath);
      System.out.println("Output: " + outFilePath);
      System.out.println("Text Out: " + outTextFilePath);
      System.out.println("\n");

      if(!adjustJob.waitForCompletion(true)) {
        System.exit(1);
      }

      iter++;
    }

    /* ==============================================
     * Print Output
     */
    try(InputStream is = fs.open(new Path(outTextFilePath, "text-m-00000"));
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
      extends Mapper<LongWritable,PRNodeWritable,LongWritable,PRNodeWritable> {

      public void map(LongWritable id, PRNodeWritable node, Context ctx)
        throws IOException, InterruptedException {
        Configuration cfg = ctx.getConfiguration();
        Long totalNodes = cfg.getLong("pagerank.total.nodes", 1);
        if(node.rank.get() < 0) {
          node.rank.set(RANK_PRECISION / totalNodes);
        }
        for(java.util.Map.Entry<Writable,Writable> edge : node.adjList.entrySet()) {
          PRNodeWritable neighbour = new PRNodeWritable();
          neighbour.id = (LongWritable) edge.getKey();
          neighbour.rank.set(node.rank.get() / node.adjList.size());
          ctx.write(neighbour.id, neighbour);
        }
        if(node.adjList.isEmpty()) {
          ctx.getCounter(COUNTER.MISSING_MASS).increment(node.rank.get());
        }
        node.rank.set(0); // don't pass previous rank
        ctx.write(id, node);
      }
  }

  public static class Reduce
      extends Reducer<LongWritable,PRNodeWritable,LongWritable,PRNodeWritable> {

      public void reduce(LongWritable id, Iterable<PRNodeWritable> nodes, Context ctx)
        throws IOException, InterruptedException {
        Configuration cfg = ctx.getConfiguration();
        Double jumpFactor = cfg.getDouble("pagerank.jump.factor", 0);
        Long totalNodes = cfg.getLong("pagerank.total.nodes", 1);
        PRNodeWritable aggrNode = new PRNodeWritable();
        aggrNode.id = id;

        long rank = 0;
        for(PRNodeWritable node : nodes) {
          if(!node.adjList.isEmpty()) {
            aggrNode.adjList.putAll(node.adjList);
          }
          rank += node.rank.get();
        }
        rank *= (1 - jumpFactor);
        rank += jumpFactor * (RANK_PRECISION / totalNodes);
        aggrNode.rank.set(rank);

        ctx.write(id, aggrNode);
      }
  }

}
