package edu.gatech.cse6242;

import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Q1 {

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "Q1");

    /* TODO: Needs to be implemented */
        job.setJarByClass(Q1.class);

       //Map-Combine-Reduce
	job.setMapperClass(EmailGraphMapper.class);
	job.setCombinerClass(EmailGraphReducer.class);
	job.setReducerClass(EmailGraphReducer.class);
    
	//Output Class
	job.setOutputKeyClass(Text.class);
	job.setOutputValueClass(IntWritable.class);

    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }


// create a class foremail mapper

public static class EmailGraphMapper extends Mapper<Object, Text, Text, IntWritable> {

		//Define the node and its weight
		private Text emailNode = new Text();
                private IntWritable weight = new IntWritable();
				
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		  
			String tokens[] = value.toString().split("\t");
			// we will take the 2nd value at index 1 target as email
			emailNode.set(tokens[1]);
			weight.set(Integer.parseInt(tokens[2]));
		        context.write(emailNode, weight); 		  
		
               }
}

  public static class EmailGraphReducer extends Reducer<Text,IntWritable,Text,IntWritable> {
    		
		private IntWritable result = new IntWritable();
    		public void reduce(Text key, Iterable<IntWritable> values,Context context) throws IOException, InterruptedException {
     		      int sum = 0;
		      for (IntWritable val : values) 
			     sum += val.get();

			result.set(sum);
			context.write(key, result); 
	}
  }







}
