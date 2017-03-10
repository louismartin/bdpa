import java.io.IOException;
import java.util.*;
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
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


public class Preprocess {


  public static HashMap<String, Integer> readWordCounts() {
      // Read word counts into a HashMap for fast membership testing
      // The hashtable underlying structure provides O(1) membership testing
      HashMap<String, Integer> wordCounts = new HashMap<String, Integer>();

      // Read csv file: inspired from https://www.mkyong.com/java/how-to-read-and-parse-csv-file-in-java/
      String csvFile = "wordcount.csv";
      BufferedReader br = null;
      String line = "";
      String cvsSplitBy = ",";

      try {
          br = new BufferedReader(new FileReader(csvFile));
          while ((line = br.readLine()) != null) {
              // use comma as separator
              String[] splittedLine = line.split(cvsSplitBy);
              wordCounts.put(splittedLine[0], Integer.parseInt(splittedLine[1].trim()));
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
      return wordCounts;
  }

  public static class LineCleanerMapper
       extends Mapper<LongWritable, Text, LongWritable, Text>{
    private Text line = new Text();
    private HashMap<String, Integer> wordCounts = Preprocess.readWordCounts();

    private class CountComparator implements Comparator<String> {
        // Custom comparator that will sort words based on their count
        @Override
        public int compare(String word1, String word2) {
            return wordCounts.get(word1).compareTo(wordCounts.get(word2));
        }
    }

    public void map(LongWritable key, Text value, Context context
                    ) throws IOException, InterruptedException {
      // Splits a string to tokens (here words).
      // We split on anything that is not an alphanumerical character
      String[] words = value.toString().toLowerCase().split("[^a-z0-9]");
      // We use the "no duplicate" property of sets
      HashSet<String> wordSet = new HashSet<String>();
      for (String word : words) {
        // Take only words that are not stopwords
        if (wordCounts.get(word) < 4000){
          wordSet.add(word);
        }
      }

      // Convert the words back to an array of strings
      String[] uniqueWords = wordSet.toArray(new String[wordSet.size()]);
      Arrays.sort(uniqueWords, new CountComparator());

      String cleanLine = new String();
      for (int i=0; i<uniqueWords.length; i++){
        cleanLine += uniqueWords[i] + " ";
      }
      cleanLine = cleanLine.trim();
      // Keep only lines that are not empty
      if (cleanLine.length() > 0){
        line.set(cleanLine.trim());
        // The key is the file byte offset which uniquely identifies a line
        context.write(key, line);
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
    conf.set("mapred.textoutputformat.separator", ",");

    Job job = Job.getInstance(conf, "Preprocessing");
    job.setJarByClass(Preprocess.class);

    // Disable the reducer, we are implementing a 'map only' job
    job.setNumReduceTasks(0);
    job.setMapperClass(LineCleanerMapper.class);

    job.setOutputKeyClass(LongWritable.class);
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
