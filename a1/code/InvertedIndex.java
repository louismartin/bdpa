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
import org.apache.hadoop.io.IntWritable;
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


public class InvertedIndex {

  public static enum WordCounter {
    WORDS_IN_UNIQUE_FILE
  };

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
       extends Mapper<Object, Text, Text, Text>{
    private Text word = new Text();
    private Text file = new Text();

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      // Get the filename (taken from stackoverflow)
      FileSplit fileSplit = (FileSplit) context.getInputSplit();
      String filename = fileSplit.getPath().getName();
      file.set(filename);
      // Get stop words
      HashSet<String> stopWords = InvertedIndex.readStopWords();

      // Splits a string to tokens (here words)
      StringTokenizer itr = new StringTokenizer(value.toString(), " .,?!\"'()[]$*-_;:|");
      String token = new String();
      while (itr.hasMoreTokens()) {
        token = itr.nextToken().toLowerCase().trim();
        if (!stopWords.contains(token)) {
          word.set(token);
          // Write one (key, value) pair to context
          context.write(word, file);
        }
      }
    }
  }

  public static class PostingListReducer
       extends Reducer<Text, Text, Text, Text> {
    private Text result = new Text();

    public void reduce(Text key, Iterable<Text> values,
                       Context context
                       ) throws IOException, InterruptedException {

      // First we are going to count each time each value appears.
      // The HashMap uses String as keys because Text does not seem to be a hashable type,
      HashMap<String, Integer> counter = new HashMap<String, Integer>();
      for (Text val : values) {
        if (counter.containsKey(val.toString())) {
          int oldValue = counter.get(val.toString());
          counter.put(val.toString(), oldValue + 1);
        } else {
          counter.put(val.toString(), 1);
        }
      }

      // Let's write the unique values and their frequency in a posting list
      // that is represented by a string (easiest).
      String postingList = new String();
      for (HashMap.Entry<String, Integer> entry : counter.entrySet()) {
        postingList += entry.getKey() + "#" + entry.getValue() + ", ";
      }
      // Remove last comma and space of string
      postingList = postingList.substring(0, postingList.length()-2);
      result.set(postingList);
      context.write(key, result);

      if (counter.size() == 1) {
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

    Job job = Job.getInstance(conf, "stop words");
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
    job.setOutputValueClass(Text.class);
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
