package srs;

import java.io.IOException;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.log4j.Logger;

public class RandSamp {
	private static final Logger sLogger = Logger.getLogger(RandSamp.class);
	public static class RandSampMapper extends
			Mapper<Object, Text, NullWritable, Text> {
		private Random rands = new Random();
		private Double percentage;

		protected void setup(Context context) throws IOException,
				InterruptedException {
			// Retrieve the percentage that is passed in via the configuration
			// like this: conf.set("filter_percentage", .5);
			// for .5%
			String strPercentage = context.getConfiguration().get(
					"filter_percentage");
			percentage = Double.parseDouble(strPercentage) / 100.0;
		}//setup

		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			if (rands.nextDouble() < percentage) {
				context.write(NullWritable.get(), value);
			}//end if
		}//end map
	}//end mapper

	public static void main(String[] args) throws Exception{

		Configuration conf = new Configuration();
		conf.set("filter_percentage", "10");//shin:sample at 10%
		
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage: BigramRelativeFrequency <in> <out>");
			System.exit(2);
		}
		int reduceTask = 1;// shin:use 1 reducer
		
		
		Job job = new Job(conf, "RandSamp");
		job.setJarByClass(RandSamp.class);
		job.setNumReduceTasks(reduceTask);// shin: set number of reducers to 1 for rand samp
		job.setMapperClass(RandSampMapper.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		job.setMapOutputKeyClass(NullWritable.class);
		job.setMapOutputValueClass(Text.class);
		FileInputFormat.setInputPaths(job, new Path(otherArgs[0]));// shin
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		long startTime = System.currentTimeMillis();
		System.exit(job.waitForCompletion(true) ? 0 : 1);
		

		sLogger.info("Job Finished in "
				+ (System.currentTimeMillis() - startTime) / 1000.0
				+ " seconds");
		
	}//end main

}//end class
