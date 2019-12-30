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

public class Q1 {
  
  public static class TokenizerMapper extends Mapper<Object, Text, IntWritable, Text>{

    private IntWritable resKey = new IntWritable();
    private Text result = new Text();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      StringTokenizer itr = new StringTokenizer(value.toString(), "\n");
      String source, target, weight;
      while (itr.hasMoreTokens()) {
        String line = itr.nextToken();
        String tokens[] = line.split("\t");
        source = tokens[0];
        target = tokens[1];
        weight = tokens[2];
	result.set(target + "," + weight);
        resKey.set(Integer.parseInt(source));
        context.write(resKey, result);
      }
    }
  }
  
  public static class IntSumReducer extends Reducer<IntWritable, Text, IntWritable, Text> {
    
    private Text results = new Text();

    public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
      int max = 0;
      int min = 1000000000;
      int target;
      int weight;
      for (Text val: values) {
	String line = val.toString();
	String tokens[] = line.split(",");
	target = Integer.parseInt(tokens[0]);
	weight = Integer.parseInt(tokens[1]);
        if (weight > max) {
	  max = weight;
	  min = target;
	}
	else if (weight == max) {
	  min = target;
	}
      }
      results.set(Integer.toString(min)+","+Integer.toString(max));
      context.write(key, results);
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "Q1");

    /* TODO: Needs to be implemented */

    job.setJarByClass(Q1.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(IntWritable.class);
    job.setOutputValueClass(Text.class);

    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
