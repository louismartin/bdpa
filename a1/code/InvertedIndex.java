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
    WORDS_IN_UNIQUE_FILE
  };

  // Creates a custom class that overrides the toString method to pretty print
  // the posting list
  public static class PostingListWritable extends MapWritable {
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

  public static HashSet<String> readStopWords() {
      // Read stopwords into a HashSet for fast membership testing
      // The hashtable underlying structure provides O(1) membership testing
      HashSet<String> stopWords = new HashSet<String>();

      // Read csv file: inspired from https://www.mkyong.com/java/how-to-read-and-parse-csv-file-in-java/
      String csvFile = "stopwords.csv";
      BufferedReader br = null;
      String line = "";
      String cvsSplitBy = ",";

      try {
          br = new BufferedReader(new FileReader(csvFile));
          while ((line = br.readLine()) != null) {
              // use comma as separator
              String[] splittedLine = line.split(cvsSplitBy);
              stopWords.add(splittedLine[0]);
          }
      } catch (FileNotFoundException e) {
          e.printStackTrace();
      } catch (IOException e) {
          e.printStackTrace();
      } finally {
          if (br != null) {
              try {
                  br.close();
              } catch (IOException e) {
                  e.printStackTrace();
              }
          }
      }
      return stopWords;
  }

  public static class TokenizerMapper
       extends Mapper<Object, Text, Text, PostingListWritable>{
    private Text word = new Text();
    private Text doc = new Text();
    private IntWritable one = new IntWritable(1);
    private PostingListWritable postingList = new PostingListWritable();
    private HashSet<String> stopWords = InvertedIndex.readStopWords();

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      FileSplit fileSplit = (FileSplit) context.getInputSplit();
      String filename = fileSplit.getPath().getName();
      doc.set(filename);

      // Splits a string to tokens (here words)
      StringTokenizer itr = new StringTokenizer(value.toString(), " .,?!\"'()[]$*-_;:|");
      String token = new String();
      while (itr.hasMoreTokens()) {
        token = itr.nextToken().toLowerCase().trim();
        if (!stopWords.contains(token)) {
          word.set(token);
          // Output is a PostingListWritable containing the filename and the value 1
          postingList.clear();
          postingList.put(doc, one);
          // Write one (key, value) pair to context
          context.write(word, postingList);
        }
      }
    }
  }

  public static class PostingListReducer
       extends Reducer<Text, PostingListWritable, Text, PostingListWritable> {
    private PostingListWritable postingList = new PostingListWritable();

    public void reduce(Text word, Iterable<PostingListWritable> postingLists,
                       Context context
                       ) throws IOException, InterruptedException {
      // We are going to aggregate all posting lists together
      postingList.clear();
      // Iterate through all posting lists comming from the mappers or the combiners
      for (PostingListWritable mw : postingLists) {
        // Iterate through the entries of one given posting list
        for (PostingListWritable.Entry<Writable, Writable> entry : mw.entrySet()) {
          Text doc = (Text) entry.getKey();
          IntWritable newValue = (IntWritable) entry.getValue();
          if (postingList.containsKey(doc)) {
            IntWritable oldValue = (IntWritable) postingList.get(doc);
            postingList.put(doc, new IntWritable(oldValue.get() + newValue.get()));
          } else {
            postingList.put(doc, new IntWritable(newValue.get()));
          }
        }
      }

      context.write(word, postingList);

      if (postingList.size() == 1) {
        // Increment counter for word appearing in a single document only.
        // We are guaranteed that this is the only moment that the counter will
        // be incremented for this word because all all the values from a given
        // key all go to the same reduce call.
        // CAREFUL: This assumption is not valid anymore if using this class as
        // a combiner too, the counter will not work.
        context.getCounter(WordCounter.WORDS_IN_UNIQUE_FILE).increment(1);
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
    // Set compression
    if ((args.length >= 5) && (Integer.parseInt(args[4]) == 1)) {
        conf.set("mapreduce.map.output.compress", "true");
    }

    Job job = Job.getInstance(conf, "Inverted Index");
    job.setJarByClass(InvertedIndex.class);

    // Set number of reducers and combiner through cli
    if (args.length >= 3) {
        job.setNumReduceTasks(Integer.parseInt(args[2]));
    }
    if ((args.length >= 4) && (Integer.parseInt(args[3]) == 1)) {
        job.setCombinerClass(PostingListReducer.class);
    }
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
      Counter wordInUniqueFileCounter = counters.findCounter(WordCounter.WORDS_IN_UNIQUE_FILE);
      String message = wordInUniqueFileCounter.getDisplayName() + ": " + wordInUniqueFileCounter.getValue();
      System.out.println(message);

      // Write these unique words to a file in HDFS
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
