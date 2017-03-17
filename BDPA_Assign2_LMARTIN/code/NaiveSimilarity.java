import java.io.IOException;
import java.util.*;
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.lang.Comparable;
import java.lang.RuntimeException;

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


public class NaiveSimilarity {

  public static ArrayList<Long> readAllKeys() {
      ArrayList<Long> keys = new ArrayList<Long>();

      // Read csv file: inspired from https://www.mkyong.com/java/how-to-read-and-parse-csv-file-in-java/
      String csvFile = "preprocess.csv";
      BufferedReader br = null;
      String line = "";
      String cvsSplitBy = ",";

      try {
          br = new BufferedReader(new FileReader(csvFile));
          while ((line = br.readLine()) != null) {
              // use comma as separator
              String[] splittedLine = line.split(cvsSplitBy);
              keys.add(Long.parseLong(splittedLine[0]));
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
      return keys;
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

  public static class PairMapper
       extends Mapper<LongWritable, Text, Text, Text>{

    private static ArrayList<Long> keys = NaiveSimilarity.readAllKeys();
    private Text pair = new Text();
    private Text currentValue = new Text();

    public void map(LongWritable key, Text value, Context context
                    ) throws IOException, InterruptedException {
      String[] split = value.toString().split(",");
      Long currentKey = Long.parseLong(split[0]);
      currentValue.set(split[1]);
      // We will compare this document to only previous documents in order to
      // only compare a given pair of documents once.
      for (Long otherKey : keys){
        // The following pair is a quick hack to pass a pair of keys, instead
        // of defining a more elegant ComparableWritable.
        // This new class would not have brought much more (YAGNI principle).
        // We put the lowest key first to be sure that the same pair of
        // documents always have the same key and thus end up in the same reducer.
        // Note that we don't take into account the case where the keys are the same.
        if (currentKey < otherKey){
          pair.set(currentKey.toString() + "\t" + otherKey.toString());
          context.write(pair, currentValue);
        } else if (currentKey > otherKey) {
          pair.set(otherKey.toString() + "\t" + currentKey.toString());
          context.write(pair, currentValue);
        }
      }
      }
    }

  public static class CompareReducer
       extends Reducer<Text, Text, Text, Text> {
    private Text value1 = new Text();
    private Text value2 = new Text();

    public void reduce(Text key, Iterable<Text> values,
                       Context context
                       ) throws IOException, InterruptedException {
      // Values is supposed to contain two elements. The content of each of the
      // two documents to be compared.
      Iterator<Text> itr = values.iterator();
      value1.set(itr.next());
      value2.set(itr.next());
      if (itr.hasNext()){
        throw new RuntimeException("More than one value for a given pair of document ids was received.");
      }
      float similarity = NaiveSimilarity.jaccard(value1.toString(), value2.toString());
      if (similarity > 0.8) {
        Text value = new Text(similarity + "\t\"" + value1.toString() + "\" - \"" + value2.toString() + "\"");
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

    Job job = Job.getInstance(conf, "Naive Similarity");
    job.setJarByClass(NaiveSimilarity.class);

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

      Counters counters = job.getCounters();
      Counter comparisonsCounter = counters.findCounter(TaskCounter.REDUCE_INPUT_GROUPS);
      System.out.println("Number of comparisons: " + comparisonsCounter.getValue());
      System.exit(0);
    }
    else {
      System.exit(1);
    }
  }

}
