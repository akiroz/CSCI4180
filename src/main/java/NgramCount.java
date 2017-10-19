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

import java.util.Map;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Iterator;
import java.util.StringTokenizer;

// Holds string value as associative array outside mapper method
// Set up global HashMap to store list values
public class NgramCount {

  // Declare global variable here
  public static LinkedList<String> list = new LinkedList<String>();

  public static void main(String[] args) throws Exception {

    Configuration conf = new Configuration();
    conf.setInt("N", Integer.parseInt(args[2]));

    Job job = new Job(conf);
    job.setJarByClass(NgramCount.class);
    job.setJobName("NgramCount");

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
        int N = context.getConfiguration().getInt("N", 2);
        StringTokenizer itr = new StringTokenizer(
            value.toString(),
            " \t\r\n\f.,></?;:'\"[]{}\\|-=_+()&*%^#$!@`~‘’“”");
            // Modified as global list
        //LinkedList<String> list = new LinkedList<String> ();
        Map<String, Integer> map = new HashMap<String, Integer>();

        while (itr.hasMoreTokens()) {
          String token = itr.nextToken();
          list.addLast(token);
          if(list.size() > N) {
            list.removeFirst();
          }
          if(list.size() == N) {
            String ngram = String.join(" ", list);
            map.put(
                ngram,
                (map.containsKey(ngram)) ? map.get(ngram) + 1 : 1);
          }
        }

        Iterator<Map.Entry<String, Integer>> it = map.entrySet().iterator();
        while(it.hasNext()) {
          Map.Entry<String, Integer> entry = it.next();
          context.write(
              new Text(entry.getKey()),
              new IntWritable(entry.getValue().intValue()));
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
