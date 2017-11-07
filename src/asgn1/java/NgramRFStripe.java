import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.IOException;

import java.util.Map;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Iterator;
import java.util.StringTokenizer;

// Holds string value as associative array outside mapper method
// Set up global HashMap to store list values
public class NgramRFStripe {

  // Declare global variable here
  public static LinkedList<String> list = new LinkedList<String>();

  public static void main(String[] args) throws Exception {

    Configuration conf = new Configuration();
    conf.setInt("N", Integer.parseInt(args[2]));

    Job job = new Job(conf);
    job.setJarByClass(NgramRFStripe.class);
    job.setJobName("NgramRFStripe");

    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));

    job.setMapperClass(MyMapper.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(MapWritable.class);

    job.setReducerClass(MyReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(FloatWritable.class);

    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }

  public static class MyMapper
      extends Mapper<Object, Text, Text, MapWritable>
    {
      private String ngram_key;

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
        Map<String, Map<String, Integer>> key_map = new HashMap<String, Map<String, Integer>>();
        Map<String, Integer> value_map = new HashMap<String, Integer>();

        while (itr.hasMoreTokens()) {
          String token = itr.nextToken();
          list.addLast(token);
          if(list.size() > N) {
            list.removeFirst();
          }
          if(list.size() == N) {
            String ngram_key = list.getFirst(); // A B C, get A
            // remove key, get B C
            list.removeFirst();
            // ngram is "B C"
            String ngram = String.join(" ", list);
            if(key_map.containsKey(ngram_key)){
              if(key_map.get(ngram_key).containsKey(ngram)){
                key_map.get(ngram_key).put(ngram,value_map.get(ngram)+1);
              }else{
                key_map.get(ngram_key).put(ngram,1);
              }
            }
          }
        }

        Iterator<Map.Entry<String, Map<String,Integer>>> it = key_map.entrySet().iterator();

        while(it.hasNext()) {
          Map.Entry<String, Map<String, Integer>> entry = it.next();

          // Convert hm to mapWritable
          MapWritable mapWritable = new MapWritable();
          for (Map.Entry<String,Integer> val_entry : entry.getValue().entrySet()) {
            if(null != val_entry.getKey() && null != val_entry.getValue()){
              mapWritable.put(new Text(val_entry.getKey()),new IntWritable(val_entry.getValue()));
            }
          }
          // Output Stripes
          context.write(
              new Text(entry.getKey()),
              mapWritable);
        }
      }
    }

  public static class MyReducer
      extends Reducer<Text, MapWritable, Text, FloatWritable>
    {
      public void reduce
        (
         Text key,
         Iterable<MapWritable> values,
         Context context
        )
        throws IOException, InterruptedException
      {
        float theta = context.getConfiguration().getFloat("theta", 0);
        Map<Text, Integer> out_map = new HashMap<Text, Integer>();

        int total = 0;

        // Something might went wrong here
        for (MapWritable entry : values) {
          total+=entry.size();
        }

        for(MapWritable entry: values){
          for(Map.Entry<Writable,Writable> pair: entry.entrySet()){
            if(out_map.containsKey(pair.getKey())){
              out_map.put(((Text)pair.getKey()),((IntWritable)pair.getValue()).get()+1);
            }else{
              out_map.put(((Text)pair.getKey()),1);
            }
          }
        }
        Iterator<Map.Entry<Text, Integer>> it = out_map.entrySet().iterator();
        while(it.hasNext()) {
          Map.Entry<Text, Integer> entry = it.next();
          float rf = ((float) entry.getValue().intValue()) /total;
          if(rf > theta) {
            context.write(
                    new Text(entry.getKey()),
                    new FloatWritable(rf));
          }
        }
    }
}
}
