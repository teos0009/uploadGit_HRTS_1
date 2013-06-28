package ind;

import io.InvertedIndex;
import io.PairOfStringInt;
import io.PairOfStrings;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;
import org.apache.hadoop.mapreduce.lib.input.KeyValueLineRecordReader;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.log4j.Logger;


/**
 * @author shin
 * 
 *
 * The Driver of MapReduce to build inverted index.
 * There are two phases before the job finished 
 * Phase1 : Build inverted index for each unique ngram from a 5gram;
 * 			secondary sorted with tf of each 5gram via map reduce pattern,
 * 			follow by list of docid in sorted order
 *          //in which termfreq, docfreq and List of (docid.tf) is set,
 * Phase2 : Build bigram base count.
 */
public class IndexDriver {

	private static final Logger sLogger = Logger.getLogger(IndexDriver.class);
	/**Index
	 * @param conf the Configuration for this job
	 * @param isSeq set <code>false</code> to get Text output , otherwise Sequence output
	 * @throws Exception 
	 */
	private static void index(Configuration conf,Boolean isSeq) throws Exception{
		Job job = new Job(conf, "BuildInvertedIndex");
		job.setNumReduceTasks(conf.getInt("numReducers",1));
		job.setJarByClass(IndexDriver.class);
		job.setMapperClass(IndexMapper.class);
		job.setReducerClass(IndexReducer.class);
		job.setCombinerClass(IndexCombiner.class);
		job.setMapOutputKeyClass(PairOfStrings.class);
		job.setMapOutputValueClass(PairOfStringInt.class);
		//job.setInputFormatClass(KeyValueTextInputFormat.class);//shin: produce no o/p using text input
		//job.setInputFormatClass(LineRecordReader.class);//method setInputFormatClass(Class<? extends InputFormat>) in the type Job is not applicable for the arguments (Class<LineRecordReader>)
		//job.setInputFormatClass(KeyValueLineRecordReader.class);////method setInputFormatClass(Class<? extends InputFormat>) in the type Job is not applicable for the arguments
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputKeyClass(PairOfStrings.class);
		job.setOutputValueClass(InvertedIndex.class);
		if (isSeq) {
			//job.setOutputFormatClass(SequenceFileOutputFormat.class);//shin:seq file as output for subsequent MR job
			job.setOutputFormatClass(TextOutputFormat.class);//shin: text for loading into redis
		}
		FileInputFormat.addInputPath(job, new Path(conf.get("input")));
		FileOutputFormat.setOutputPath(job, new Path(conf.get("output")));

		Path outputDir = new Path(conf.get("output"));
		FileSystem.get(outputDir.toUri(),conf).delete(outputDir, true);

		long startTime = System.currentTimeMillis();
		job.waitForCompletion(true);
		sLogger.info(job.getJobName() + " Finished in "
				+ (System.currentTimeMillis() - startTime) / 1000.0
				+ " seconds");
		
	}

	public static void main(String[] args) throws Exception {
		
		if(args.length!=3){
			System.out.println("[input] [output] [numberofreudcers]");
			return;
		}
		Configuration p1conf = new Configuration();
		p1conf.set("input", args[0]);
		p1conf.set("output",args[1]+"/gram");
		p1conf.setInt("numReducers", Integer.parseInt(args[2]));
		p1conf.setInt("indextype", InvertedIndex.TYPE_BIGRAM);//shin:obj from InvertedIndex.java

		index(p1conf,true);
		
//		//shin: not needed in this use case
//		Configuration p2conf = new Configuration();
//		p2conf.set("input", args[0]);
//		p2conf.set("output",args[1]+"/base");
//		p2conf.setInt("numReducers", Integer.parseInt(args[2]));
//		p2conf.setInt("indextype", InvertedIndex.TYPE_BASE);
//		index(p2conf,true);
		
	
		
		

	}

}
