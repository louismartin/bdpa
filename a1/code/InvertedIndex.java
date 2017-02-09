import java.io.IOException;
import java.util.StringTokenizer;
import java.util.HashSet;
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


public class InvertedIndex {

  public static class TokenizerMapper
       extends Mapper<Object, Text, Text, Text>{
    private Text word = new Text();
    private Text file = new Text();

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

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      // Get the filename (taken from stackoverflow)
      FileSplit fileSplit = (FileSplit) context.getInputSplit();
      String filename = fileSplit.getPath().getName();
      file.set(filename);
      // Get stop words
      HashSet<String> stopWords = TokenizerMapper.readStopWords();

      // Splits a string to tokens (here words)
      StringTokenizer itr = new StringTokenizer(value.toString(), " .,?!");
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

      String postingList = new String();
      for (Text val : values) {
        postingList += val + ", ";
      }
      // Remove last two characters of string
      postingList = postingList.substring(0, postingList.length()-2);
      result.set(postingList);
      context.write(key, result);
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();

    // Set separator to write as a csv file
    conf.set("mapred.textoutputformat.separator", ": ");
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
      System.exit(0);
    }
    else {
      System.exit(1);
    }
  }
}
