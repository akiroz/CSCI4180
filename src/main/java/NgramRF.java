import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;

import java.util.Map;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Iterator;
import java.util.StringTokenizer;


public class NgramRF {
  // Declare global variable here
  public static LinkedList<String> list = new LinkedList<String>();

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    conf.setInt("N", Integer.parseInt(args[2]));
    conf.setFloat("theta", Float.parseFloat(args[3]));

    Job job = new Job(conf);
    job.setJarByClass(NgramRF.class);
    job.setJobName("NgramRF");

    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));

    job.setMapperClass(MyMapper.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);

    job.setReducerClass(MyReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(FloatWritable.class);

    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }

  public static class MyMapper
      extends Mapper<LongWritable, Text, Text, Text>
    {
      public void map
        (
         LongWritable key,
         Text value,
         Context context
        )
        throws IOException, InterruptedException
      {
        int N = context.getConfiguration().getInt("N", 0);
        StringTokenizer itr = new StringTokenizer(
            value.toString(),
            " \t\n\r\f.,></?;:'\"[]{}\\|-=_+()&*%^#$!@`~‘’“”");
        // Use Global var instead
        //LinkedList<String> list = new LinkedList<String> ();

        // Below is  the pair approach
        while (itr.hasMoreTokens()) {
          list.addLast(itr.nextToken());
          if(list.size() > N) {
            list.removeFirst();
          }
          if(list.size() == N) {
            context.write(
                new Text(list.getFirst()),
                new Text(String.join(" ", list)));
          }
        }

      }
    }


  // All key value pairs with the same key goes to the same reducer
  public static class MyReducer
      extends Reducer<Text, Text, Text, FloatWritable>
    {
      public void reduce
        (
         Text key,
         Iterable<Text> values,
         Context context
        )
        throws IOException, InterruptedException
      {
        float theta = context.getConfiguration().getFloat("theta", 0);
        // TotalCount for all pairs with the same key
        int totalCount = 0;
        Map<String, Integer> map = new HashMap<String, Integer>();

        for (Text val : values) {
          totalCount++;
          String valStr = val.toString();
          map.put(
              valStr,
              (map.containsKey(valStr)) ? map.get(valStr) + 1 : 1);
        }

        Iterator<Map.Entry<String, Integer>> it = map.entrySet().iterator();
        while(it.hasNext()) {
          Map.Entry<String, Integer> entry = it.next();
          float rf = ((float) entry.getValue().intValue()) / totalCount;
          if(rf > theta) {
            context.write(
                new Text(entry.getKey()),
                new FloatWritable(rf));
          }
        }
      }
    }
}
