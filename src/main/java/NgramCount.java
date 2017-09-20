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
import java.io.Reader;
import java.io.StringReader;

import java.util.Map;
import java.util.HashMap;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.standard.StandardTokenizer;
import org.apache.lucene.analysis.shingle.ShingleFilter;

public class NgramCount {
  private static int N;
  public static void main(String[] args) throws Exception {
    N = Integer.parseInt(args[2]);

    Job job = new Job(new Configuration());
    job.setJarByClass(NgramCount.class);
    job.setJobName("NgramCount");

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
        Map<String, Integer> map = new HashMap<String, Integer>();

        NGramTokenizer gramTokenizer = new NGramTokenizer(N,N);
        gramTokenizer.setReader(new StringReader(value.toString()));
        CharTermAttribute charTermAttribute = gramTokenizer.addAttribute(CharTermAttribute.class);
        gramTokenizer.reset();

        while (gramTokenizer.incrementToken()) {
          String token = charTermAttribute.toString();
          map.put(token, (map.containsKey(token)) ? map.get(token) + 1 : 1);
        }
        gramTokenizer.end();
        gramTokenizer.close();

        Iterator<Map.Entry<String, Integer>> it = map.entrySet().iterator();
        while(it.hasNext()) {
          Map.Entry<String, Integer> entry = it.next();
          context.write(
              new Text(entry.getKey()),
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
