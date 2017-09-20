import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.util.Map;
import java.util.HashMap;
import java.util.Iterator;
import java.util.StringTokenizer;

public class WordLengthCount {
  public static void main(String[] args) throws Exception {
    Job job = new Job(new Configuration());
    job.setJarByClass(WordLengthCount.class);
    job.setJobName("WordLengthCount");

    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));

    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);

    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }

  public static class TokenizerMapper
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
        Map<Integer, Integer> map = new HashMap<Integer, Integer>();

        while (itr.hasMoreTokens()) {
          int length = itr.nextToken().length();
          map.put(
              length,
              (map.containsKey(length)) ? map.get(length) + 1 : 1);
        }

        Iterator<Map.Entry<Integer, Integer>> it = map.entrySet().iterator();
        while(it.hasNext()) {
          Map.Entry<Integer, Integer> entry = it.next();
          context.write(
              new Text(Integer.toString(entry.getKey())),
              new IntWritable(entry.getValue().intValue()));
        }
      }
    }

  public static class IntSumReducer
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
