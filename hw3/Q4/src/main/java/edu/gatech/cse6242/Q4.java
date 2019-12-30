package edu.gatech.cse6242;

import java.io.IOException;
import java.util.StringTokenizer;
import java.lang.Object;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.IOException;

public class Q4 {


  public static class DegreeMapper extends Mapper<Object, Text, IntWritable, IntWritable> {
    
    private final static IntWritable one = new IntWritable(1);
    private final static IntWritable negOne = new IntWritable(-1);
    private IntWritable node = new IntWritable();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      StringTokenizer itr = new StringTokenizer(value.toString());
      int count  = 0;
      while (itr.hasMoreTokens()) {
        node.set(Integer.parseInt(itr.nextToken()));
        if (count == 0) {
          context.write(node, one);
          count = 1;
        }
        else {
          context.write(node, negOne);
          count = 0;
        }
      }
    }
  }

  public static class DegreeReducer extends Reducer<IntWritable,IntWritable,IntWritable,IntWritable> {
    
    private IntWritable result = new IntWritable();

    public void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
      int diff = 0;
      for (IntWritable val: values) {
        diff += val.get();
      }
      result.set(diff);
      context.write(key, result);
    }
  }

  public static class CountMapper extends Mapper<Object, Text, IntWritable, IntWritable> {

    private final static IntWritable one = new IntWritable(1);
    private IntWritable diff = new IntWritable();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      StringTokenizer itr = new StringTokenizer(value.toString(), "\n");
      while (itr.hasMoreTokens()) {
        String line = itr.nextToken();
        String tokens[] = line.split("\t");
        diff.set(Integer.parseInt(tokens[1]));
        context.write(diff, one);
      }
    }
  }

  public static class CountReducer extends Reducer<IntWritable,IntWritable,IntWritable,IntWritable> {

    private IntWritable result = new IntWritable();

    public void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable val : values) {
        sum += val.get();
      }
      result.set(sum);
      context.write(key, result);
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "job1");

    /* TODO: Needs to be implemented */

    job.setJarByClass(Q4.class);
    job.setMapperClass(DegreeMapper.class);
    job.setCombinerClass(DegreeReducer.class);
    job.setReducerClass(DegreeReducer.class);
    job.setOutputKeyClass(IntWritable.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path("firstJobOutput"));
    job.waitForCompletion(true);

    Job job2 = Job.getInstance(conf, "job2");
    job2.setJarByClass(Q4.class);
    job2.setMapperClass(CountMapper.class);
    job2.setCombinerClass(CountReducer.class);
    job2.setReducerClass(CountReducer.class);
    job2.setOutputKeyClass(IntWritable.class);
    job2.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job2, new Path("firstJobOutput"));
    FileOutputFormat.setOutputPath(job2, new Path(args[1]));

    System.exit(job2.waitForCompletion(true) ? 0 : 1);
  }
}
