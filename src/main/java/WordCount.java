import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;

import java.util.StringTokenizer;


public class WordCount {
  public static void main(String[] args) throws Exception {

    Configuration conf = new Configuration();

    Job job = new Job(conf);
    job.setJarByClass(WordCount.class);
    job.setJobName("WordCount");

    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));

    job.setMapperClass(MyMapper.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(IntWritable.class);

    job.setReducerClass(MyReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);

    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }

  public static class MyMapper
      extends Mapper<Object, Text, Text, IntWritable>
    {
      public void map
        (
         Object key,
         Text value,
         Context context
        )
        throws IOException, InterruptedException
      {
        StringTokenizer itr = new StringTokenizer(value.toString());
        
        while (itr.hasMoreTokens()) {
          context.write(
              new Text(itr.nextToken()),
              new IntWritable(1));
        }
      }
    }

  public static class MyReducer
      extends Reducer<Text, IntWritable, Text, IntWritable>
    {
      public void reduce
        (
         Text key,
         Iterable<IntWritable> values,
         Context context
        )
        throws IOException, InterruptedException
      {
        int sum = 0;
        for (IntWritable val : values) {
          sum += val.get();
        }
        context.write(
            key,
            new IntWritable(sum));
      }
    }
}
