import java.io.IOException;
import java.util.*;
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.lang.Comparable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.lib.join.TupleWritable;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.TaskCounter;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataOutputStream;


public class Similarity {

  // NOT USED, COULD NOT MAKE IT WORK
  public static class LongArrayWritable extends ArrayWritable implements Comparable<LongArrayWritable> {
      // For storing pair of keys
      public LongArrayWritable(LongWritable[] values) {
          super(LongWritable.class, values);
      }

      @Override
      public LongWritable[] get() {
          return (LongWritable[]) super.get();
      }

      @Override
      public int compareTo(LongArrayWritable other) {
        // Sort first on first element and on second only if first elements are equal
        long thisValue1 = this.get()[0].get();
        long thatValue1 = other.get()[0].get();;
        if (thisValue1 < thatValue1) {
          return -1;
        } else if (thisValue1 > thatValue1) {
          return 1;
        } else {
          long thisValue2 = this.get()[1].get();;
          long thatValue2 = other.get()[1].get();;
          return (thisValue2 < thatValue2 ? -1 : (thisValue2==thatValue2 ? 0 : 1));
        }
      }

      @Override
      public String toString() {
          LongWritable[] values = get();
          return "(" + values[0].toString() + ", " + values[1].toString() + ")";
      }


  }


  public static class PairMapper
       extends Mapper<LongWritable, Text, Text, Text>{

    private static ArrayList<LongWritable> previousIds = new ArrayList<LongWritable>();
    private Text pair = new Text();
    public void map(LongWritable key, Text value, Context context
                    ) throws IOException, InterruptedException {
      for (LongWritable previousId : previousIds){
        // Dirty hack to pass a pair of keys, bypasses the definition of a new
        // ComparableWritable class which I could not successfully implement
        // in reasonable time.
        pair.set(key.toString() + "\t" + previousId.toString());
        context.write(pair, value);
      }
      // We need to create a new object else we would save a list of pointers to the same object
      LongWritable savedKey = new LongWritable(key.get());
      previousIds.add(savedKey);
      }
    }

  public static class CompareReducer
       extends Reducer<Text, Text, Text, Text> {
    public void reduce(Text key, Iterable<Text> values,
                       Context context
                       ) throws IOException, InterruptedException {

      for (Text value : values){
        context.write(key, value);
      }
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    // Remove output folder if it exists
    Path output = new Path(args[1]);
    FileSystem hdfs = FileSystem.get(conf);
    // delete existing directory
    if (hdfs.exists(output)) {
        hdfs.delete(output, true);
    }

    // Set separator to write as a csv file
    conf.set("mapred.textoutputformat.separator", " : ");

    Job job = Job.getInstance(conf, "Similarity");
    job.setJarByClass(Similarity.class);

    job.setMapperClass(PairMapper.class);
    job.setReducerClass(CompareReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    long startTime = System.nanoTime();
    if (job.waitForCompletion(true)) {
      long endTime = System.nanoTime();
      float duration = (endTime - startTime);
      duration /= 1000000000;
      System.out.println("***** Elapsed: " + duration + "s *****\n");
      System.exit(0);
    }
    else {
      System.exit(1);
    }
  }

}
