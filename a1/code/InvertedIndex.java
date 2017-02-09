import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class StopWords {

  public static class TokenizerMapper
       extends Mapper<Object, Text, Text, IntWritable>{

    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      // Splits a string to tokens (here words)
      StringTokenizer itr = new StringTokenizer(value.toString());
      while (itr.hasMoreTokens()) {
        word.set(itr.nextToken());
        // Write one (key, value) pair to context
        context.write(word, one);
      }
    }
  }

  public static class IntSumReducer
       extends Reducer<Text,IntWritable,Text,IntWritable> {
    private IntWritable result = new IntWritable(10);

    public void reduce(Text key, Iterable<IntWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable val : values) {
        sum += val.get();
      }
      // Only write the key value pair if its frequency is high enough
      if (sum > 4000){
        result.set(sum);
        context.write(key, result);
      }
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    // Ressource manager configurations
    //conf.set("fs.defaultFS", "hdfs://127.0.0.1:9000");
    //conf.set("mapreduce.jobtracker.address", "localhost:54311");
    //conf.set("mapreduce.framework.name", "yarn");
    //conf.set("yarn.resourcemanager.address", "localhost:8032");

    // Set separator to write as a csv file
    conf.set("mapred.textoutputformat.separator", ", ");
    // Set compression
    if ((args.length >= 5) && (Integer.parseInt(args[4]) == 1)) {
        conf.set("mapreduce.map.output.compress", "true");
    }

    Job job = Job.getInstance(conf, "stop words");
    job.setJarByClass(StopWords.class);

    // Set number of reducers and combiner through cli
    if (args.length >= 3) {
        job.setNumReduceTasks(Integer.parseInt(args[2]));
    }
    if ((args.length >= 4) && (Integer.parseInt(args[3]) == 1)) {
        job.setCombinerClass(IntSumReducer.class);
    }
    job.setMapperClass(TokenizerMapper.class);
    job.setReducerClass(IntSumReducer.class);

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
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
