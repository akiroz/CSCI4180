
import java.io.IOException;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import org.apache.hadoop.io.LongWritable;

public class PRAdjust {

  public static class Map
      extends Mapper<LongWritable,PRNodeWritable,LongWritable,PRNodeWritable> {
  }
 
  public static class Reduce
      extends Reducer<LongWritable,PRNodeWritable,LongWritable,PRNodeWritable> {
  }

}
