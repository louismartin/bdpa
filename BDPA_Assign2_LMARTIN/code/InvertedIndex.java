import java.io.IOException;
import java.lang.Math;
import java.util.*;
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
  public static double threshold = 0.5;


  public static HashMap<Long, String> readAllDocs() {
      // Read all documents into a HashMap for fast element retrieval.
      // The underlying hashtable structure provides O(1) element retrival.
      HashMap<Long, String> docs = new HashMap<Long, String>();

      // Read csv file: inspired from https://www.mkyong.com/java/how-to-read-and-parse-csv-file-in-java/
      String csvFile = "preprocess.csv";
      BufferedReader br = null;
      String line = "";

      try {
          br = new BufferedReader(new FileReader(csvFile));
          while ((line = br.readLine()) != null) {
              // use comma as separator
              String[] splittedLine = line.split(",");
              docs.put(Long.parseLong(splittedLine[0]), splittedLine[1].trim());
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
      return docs;
  }


  public static float jaccard(String doc1, String doc2){
    Set words1 = new HashSet<String>(Arrays.asList(doc1.split(" ")));
    Set words2 = new HashSet<String>(Arrays.asList(doc2.split(" ")));
    Set intersection = new HashSet<String>(words1);
    intersection.retainAll(words2);
    Set union = new HashSet<String>(words1);
    union.addAll(words2);
    float similarity = (float) intersection.size() / (float) union.size();
    return similarity;
  }


  public static class TokenizerMapper
       extends Mapper<Object, Text, Text, LongWritable>{
    private Text word = new Text();
    private LongWritable docKey = new LongWritable();

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      String[] split = value.toString().split(",");
      docKey.set(Long.parseLong(split[0]));
      // Splits a string to tokens (here words)
      String[] words = split[1].split(" ");
      int cutoff = words.length - (int) Math.ceil((double) words.length * threshold) + 1;
      for (int i=0; i < cutoff; i++) {
          word.set(words[i]);
          context.write(word, docKey);
      }
    }
  }


  public static class PostingListReducer
       extends Reducer<Text, LongWritable, Text, Text> {
    private Text outputKey = new Text();
    private Text outputValue = new Text();
    private HashMap<Long, String> allDocs = readAllDocs();

    public void reduce(Text word, Iterable<LongWritable> docKeys,
                       Context context
                       ) throws IOException, InterruptedException {
      ArrayList<LongWritable> processedKeys = new ArrayList<LongWritable>();
      String doc1 = new String();
      String doc2 = new String();

      for (LongWritable docKey : docKeys) {
        for (LongWritable processedKey : processedKeys) {
          doc1 = allDocs.get(processedKey.get());
          doc2 = allDocs.get(docKey.get());
          float similarity = InvertedIndex.jaccard(doc1, doc2);
          if (similarity > threshold){
            outputKey.set("(" + processedKey.toString() + ", " + docKey.toString() + ")");
            outputValue.set(similarity + "\t\"" + doc1 + "\" - \"" + doc2 + "\"");
            context.write(outputKey, outputValue);
          }
        }

        LongWritable processedKey = new LongWritable(docKey.get());
        processedKeys.add(processedKey);
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
    job.setOutputValueClass(LongWritable.class);
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
