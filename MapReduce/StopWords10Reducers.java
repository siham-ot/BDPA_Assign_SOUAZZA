package edu.stanford.cs246.wordcount;

import java.io.IOException;
import java.util.regex.Pattern;
import java.io.IOException;
import java.util.Arrays;
import org.apache.log4j.Logger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import edu.stanford.cs246.wordcount.WordCount.Map;
import edu.stanford.cs246.wordcount.WordCount.Reduce;

public class StopWords10Reducers extends Configured implements Tool {
	  public static void main(String[] args) throws Exception {
		    int res = ToolRunner.run(new StopWords10Reducers(), args);
		    System.exit(res);
		  }
	  
	  @Override
	   public int run(String[] args) throws Exception {
	      System.out.println(Arrays.toString(args));
	      Job job = new Job(getConf(), "StopWords10Reducers");
	      
	      job.setJarByClass(StopWords10Reducers.class);
	      job.setOutputKeyClass(Text.class);
	      job.setOutputValueClass(IntWritable.class);

	      job.setMapperClass(Map.class);
	      job.setReducerClass(Reduce.class);

	      job.setInputFormatClass(TextInputFormat.class);
	      job.setOutputFormatClass(TextOutputFormat.class);
	      
	      job.getConfiguration().set(
				"mapreduce.output.textoutputformat.separator", ",");
		  job.setNumReduceTasks(10);

	      FileInputFormat.addInputPath(job, new Path(args[0]));
	      FileOutputFormat.setOutputPath(job, new Path(args[1]));


		  FileSystem txtfile = FileSystem.newInstance(getConf());
	      if (txtfile.exists(new Path(args[1]))) {
	    	  txtfile.delete(new Path(args[1]), true);
			}

	      return job.waitForCompletion(true) ? 0 : 1;
	   }
	  

public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
		private final static IntWritable ONE = new IntWritable(1);
		private Text word = new Text();

		@Override
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			for (String token : value.toString().split("\\s+")) {
				word.set(token.toLowerCase());
				context.write(word, ONE);
			}
		}
	}
	  

public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {
	@Override
	public void reduce(Text key, Iterable<IntWritable> values,
			Context context) throws IOException, InterruptedException {
		int sum = 0;
		for (IntWritable val : values) {
			sum += val.get();
		}
		if (sum > 4000) {
			context.write(key, new IntWritable(sum));
		}
	}
}
}
