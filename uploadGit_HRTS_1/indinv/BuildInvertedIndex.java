package indinv;

//======new api style==========

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;//shin
import java.util.Map;
import java.util.StringTokenizer;
import java.util.Map.Entry;
import java.lang.String;//shin

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;//shin
import org.apache.hadoop.fs.FileSystem;//shin
import org.apache.hadoop.fs.FileStatus;//shin
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;//shin
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;//shin
import org.apache.hadoop.io.FloatWritable;//shin
import org.apache.hadoop.io.WritableComparable;//shin
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Partitioner;//shin
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;//shin
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;//shin
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;//shin
import org.apache.hadoop.io.MapFile;

import org.apache.hadoop.util.Tool;//shin
import org.apache.hadoop.util.ToolRunner;//shin
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.log4j.Logger;//shin

import io.ArrayListWritable;//shin
import io.InvertedIndex;
import io.PairOfInts;//shin
import io.PairOfStrings;
import io.PairOfStringInt;

public class BuildInvertedIndex extends Configured {// shin
  private static final Logger sLogger = Logger.getLogger(BuildInvertedIndex.class);

  //private static class myMapper extends Mapper<LongWritable, Text, Text, PairOfInts> {
  private static class myMapper extends Mapper<Object, Text, PairOfStrings,PairOfStringInt> {
    //private static final Text theWord = new Text();//shin: the terms we want to invind
  	private static PairOfStrings ngram = new PairOfStrings();//shin: holds bi, tri, quad gram in LHS, RHS store as empty
	private static String tf;//MR pattern, sort by tf during shuffle sort
	private static PairOfStringInt indexitem = new PairOfStringInt(); //(docid,1); 1 is the count
      public void map(
              LongWritable docno,
              Text doc,
              Context context) throws IOException, InterruptedException {
          //rip Out the words from text in each doc#, emit as <text, i,j>;i=doc#,j=tf

          String txt = doc.toString();//shin: lines of text
          String splitPattern = "\\s+";
          String[] terms = txt.split(splitPattern);//the words

          for (int i = 0; i < terms.length; i++) {
              if (terms[i].length() == 0) {
                  continue;//shin : loop thru empty
              }
              if (terms[i] == null) {
                  continue;//shin: loop thru empty
              }
          }//end for

          //emit
//          for (PairOfObjectInt<String> e : tf) {
//              theWord.set(e.getLeftElement());
//              context.write(theWord, new PairOfInts((int) docno.get(), e.getRightElement()));
//          }//end for      
          
        int i = 0;  
  		tf = terms[terms.length-1];//tf from 5gram used in sorting
  		String bigram = terms[i] + " " + terms[i + 1];
  		String trigram = terms[i] + " " + terms[i + 1]+ " "+ terms[i + 2];
  		String quadgram = terms[i] + " " + terms[i + 1]+ " "+terms[i + 2]+" "+terms[i + 3];
		
  		// set (ngram, tf),key
		ngram.set(bigram,terms[terms.length]);
		context.write(ngram, indexitem);
		
		ngram.set(trigram,terms[terms.length]);
		context.write(ngram, indexitem);
		
		ngram.set(quadgram,terms[terms.length]);
		context.write(ngram, indexitem);
      }// end map func
  }// end mapper
  // reducer for inv ind
  //private static class myReducer extends Reducer<Text, PairOfInts, Text, PairOfWritables<IntWritable, ArrayListWritable<PairOfInts>>> {
  private static class myReducer extends Reducer<PairOfStrings, PairOfStringInt, PairOfStrings, InvertedIndex>{
		private static InvertedIndex invertedindex = new InvertedIndex();

		private static int indexType;  
		protected void reduce(PairOfStrings key, Iterable<PairOfStringInt> value,
				Context context) throws IOException, InterruptedException {
			int termfreq = 0;

          Map<String, Integer> indexitems = new HashMap<String, Integer>();
			for (PairOfStringInt v : value) {
				if (!indexitems.containsKey(v.getLeftElement())) {//shin: left is docid, right is count=1
					indexitems.put(v.getLeftElement(), v.getRightElement());
				} else {
					//shin: not needed for this use case
					int tf = indexitems.get(v.getLeftElement());
					indexitems.put(v.getLeftElement(), tf + v.getRightElement());
				}
				termfreq += v.getRightElement();

			}//end for list of values

			invertedindex.setTermfreq(termfreq);
			invertedindex.setType(indexType);
			ArrayListWritable<PairOfStringInt> indexlist = new ArrayListWritable<PairOfStringInt>();
			for (Entry<String, Integer> e : indexitems.entrySet()) {
				indexlist.add(new PairOfStringInt(e.getKey(), e.getValue()));
			}
			// sort
			Collections.sort(indexlist);
			invertedindex.setIndex(indexlist);
			context.write(key, invertedindex);
	  
      }// end reduce
  }// end FloatSumReducer for bigram freq


  public static void main(String[] args) throws Exception {
      Configuration conf = new Configuration();
      String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
      if (otherArgs.length != 2) {
          System.err.println("Usage: BuildInvertedIndex <in> <out>");
          System.exit(2);
      }
      int reduceTask = 1;// shin:use 1 reducer
      int mapTask = 8;// shin:use 8 mapper
      sLogger.info("Tool name: BuildInvertedIndex");
      sLogger.info(" - input path: " + otherArgs[0]);
      sLogger.info(" - output path: " + otherArgs[1]);
      sLogger.info(" - num mappers: " + mapTask);
      sLogger.info(" - num reducers: " + reduceTask);

      // Job job = new Job(conf, "word count");//default
      Job job = new Job(conf, "BuildInvertedIndex");
      job.setJarByClass(BuildInvertedIndex.class);
      job.setNumReduceTasks(reduceTask);// shin: set number of reducers
      
/*
      job.setMapperClass(myMapper.class);//shin: invind
      job.setReducerClass(myReducer.class);//shin: invind
      job.setMapOutputKeyClass(Text.class);// shin: longwritable then can work
      job.setMapOutputValueClass(PairOfInts.class);// shin: Text then can work
      
      // Set the outputs for the Job
      job.setOutputKeyClass(Text.class);//shin
      job.setOutputValueClass(PairOfWritables.class);// shin: invind      
      job.setOutputFormatClass(TextOutputFormat.class);
 */   
		job.setMapOutputKeyClass(PairOfStrings.class);
		job.setMapOutputValueClass(PairOfStringInt.class);
		job.setInputFormatClass(KeyValueTextInputFormat.class);
		job.setOutputKeyClass(PairOfStrings.class);
		job.setOutputValueClass(InvertedIndex.class);
      
      FileInputFormat.setInputPaths(job, new Path(otherArgs[0]));// shin
      FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
      System.exit(job.waitForCompletion(true) ? 0 : 1);
  }//end main
}// end class
