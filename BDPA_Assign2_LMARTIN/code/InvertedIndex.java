import java.io.IOException;
import java.util.StringTokenizer;
import java.util.HashSet;
import java.util.HashMap;
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.MapWritable;
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


public class InvertedIndex {

  public static enum WordCounter {
    WORDS_IN_UNIQUE_DOC
  };


  public static class PostingListWritable extends MapWritable {
    // Creates a custom class that overrides the toString method to pretty
    // print the posting list
    @Override
    public String toString() {
      String stringRepr = new String();
      for (PostingListWritable.Entry<Writable, Writable> entry : this.entrySet()) {
        stringRepr += entry.getKey() + "#" + entry.getValue() + ", ";
      }
      // Remove last comma and space of string
      stringRepr = stringRepr.substring(0, stringRepr.length()-2);
      return stringRepr;
    }
  }


  public static class TokenizerMapper
       extends Mapper<Object, Text, Text, PostingListWritable>{
    private Text word = new Text();
    private LongWritable lineKey = new LongWritable();
    private IntWritable one = new IntWritable(1);
    private PostingListWritable postingList = new PostingListWritable();

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      String[] split = value.toString().split(",");
      lineKey.set(Long.parseLong(split[0]));
      // Splits a string to tokens (here words)
      StringTokenizer itr = new StringTokenizer(split[1], " ");
      while (itr.hasMoreTokens()) {
          word.set(itr.nextToken());
          // Output is a PostingListWritable containing the filename and the value 1
          postingList.clear();
          postingList.put(lineKey, one);
          // Write one (key, value) pair to context
          context.write(word, postingList);
      }
    }
  }


  public static class PostingListCombiner
       extends Reducer<Text, PostingListWritable, Text, PostingListWritable> {
    private PostingListWritable postingList = new PostingListWritable();
    protected boolean isLastWordInUniqueDoc = false;

    public void reduce(Text word, Iterable<PostingListWritable> postingLists,
                       Context context
                       ) throws IOException, InterruptedException {
      // We are going to aggregate all posting lists together
      postingList.clear();
      // Iterate through all posting lists comming from the mappers or the combiners
      for (PostingListWritable mw : postingLists) {
        // Iterate through the entries of one given posting list
        for (PostingListWritable.Entry<Writable, Writable> entry : mw.entrySet()) {
          LongWritable lineKey = (LongWritable) entry.getKey();
          IntWritable newValue = (IntWritable) entry.getValue();
          if (postingList.containsKey(lineKey)) {
            IntWritable oldValue = (IntWritable) postingList.get(lineKey);
            postingList.put(lineKey, new IntWritable(oldValue.get() + newValue.get()));
          } else {
            postingList.put(lineKey, new IntWritable(newValue.get()));
          }
        }
      }

      isLastWordInUniqueDoc = (postingList.size() == 1);
      context.write(word, postingList);
    }
  }


  public static class PostingListReducer
       extends PostingListCombiner {
    // The only thing different between the reducer and the combiner is the
    // counter incrementation. It must be incremented only once per key, hence in
    // the reducer.
    @Override
    public void reduce(Text word, Iterable<PostingListWritable> postingLists,
                       Context context
                       ) throws IOException, InterruptedException {
         super.reduce(word, postingLists, context);
         if (isLastWordInUniqueDoc) {
           // Increment counter for word appearing in a single document only
           // We are guaranteed that this is the only time that the counter
           // will be incremented for this word because all the values from
           // a given key all go to the same reduce call.
           context.getCounter(WordCounter.WORDS_IN_UNIQUE_DOC).increment(1);
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

    Job job = Job.getInstance(conf, "Inverted Index");
    job.setJarByClass(InvertedIndex.class);

    // Set number of reducers and combiner through cli
    job.setMapperClass(TokenizerMapper.class);
    job.setReducerClass(PostingListReducer.class);

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(PostingListWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    long startTime = System.nanoTime();
    if (job.waitForCompletion(true)) {
      long endTime = System.nanoTime();
      float duration = (endTime - startTime);
      duration /= 1000000000;
      System.out.println("***** Elapsed: " + duration + "s *****\n");

      // Retrieve unique words from counter
      Counters counters = job.getCounters();
      Counter uniqueKeysCounter = counters.findCounter(TaskCounter.REDUCE_INPUT_GROUPS);
      System.out.println("Unique words: " + uniqueKeysCounter.getValue());
      Counter wordInUniqueFileCounter = counters.findCounter(WordCounter.WORDS_IN_UNIQUE_DOC);
      String message = wordInUniqueFileCounter.getDisplayName() + ": " + wordInUniqueFileCounter.getValue();
      System.out.println(message);

      // Write the count of unique words to a file in HDFS
      Path filePath = new Path("words_in_unique_file.txt");
      if (hdfs.exists(filePath)) {
          hdfs.delete(filePath, true);
      }
      FSDataOutputStream fin = hdfs.create(filePath);
      fin.writeUTF(message);
      fin.close();

      System.exit(0);
    }
    else {
      System.exit(1);
    }
  }
}
