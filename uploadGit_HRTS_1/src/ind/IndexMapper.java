package ind;

import io.PairOfStringInt;
import io.PairOfStrings;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * @author shin
 * This is the mapper used for building inverted index 
 * for each ngram <code>emit(bigram , {docid , 1 }); emitted as context.write(bigram, indexitem);
 */
public class IndexMapper extends
		Mapper<Object, Text, PairOfStrings,PairOfStringInt> {

	private static PairOfStrings bigram = new PairOfStrings();
	private static PairOfStrings ngram = new PairOfStrings();//shin: holds bi, tri, quad gram in LHS, RHS store as empty
	private static PairOfStringInt indexitem = new PairOfStringInt(); //(docid,1); 1 is the count
	private static int indexType ;
	private static int tf;//MR pattern, sort by tf during shuffle sort

	
	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		indexType = context.getConfiguration().getInt("indextype",-1);
		
	}
	
//sample 5gram with and without punc marks|follow by tf
//the legend about the invincibility	85
//the legend - Lou Franceschetti	141

	@Override
	protected void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {
		// Using regular expression to tokenize sentences.
		String terms[] = value.toString().split("[^a-zA-Z0-9']");
		
		//split terms by white space
		//String terms[] = value.toString().split("\\s+");
		
		if (terms.length < 4) {//using 5 gram
			return;
		}
		int i = 0;
		tf = Integer.parseInt(terms[terms.length-1]);//tf from 5gram used in sorting
		String bigram = terms[i] + " " + terms[i + 1];
		String trigram = terms[i] + " " + terms[i + 1]+ " "+ terms[i + 2];
		String quadgram = terms[i] + " " + terms[i + 1]+ " "+terms[i + 2]+" "+terms[i + 3];
		
	
		//F1 style
		if(indexType==0){//shin: type bigram
			indexitem.set(key.toString(),1);//key from mapper input
			
			//lots of network activity with this style
			//tx bi, tri, quad; 
			
			//shin: set (gram,*) to count freq, but not applicable to this 5gram use case
//			ngram.set(bigram, "*");
//			context.write(ngram, indexitem);
//			ngram.set(trigram, "*");
//			context.write(ngram, indexitem);
//			ngram.set(quadgram, "*");
//			context.write(ngram, indexitem);
			
			// set (ngram, tf),key
			ngram.set(bigram,terms[terms.length-1]);
			context.write(ngram, indexitem);
			
			ngram.set(trigram,terms[terms.length-1]);
			context.write(ngram, indexitem);
			
			ngram.set(quadgram,terms[terms.length-1]);
			context.write(ngram, indexitem);
			
		}else if(indexType == 1 ){//shin: type base
			ngram.set("*", "*");
			context.write(ngram, indexitem);
//			ngram.set(terms[i], "*");
//			context.write(ngram, indexitem);
			
			ngram.set(bigram,terms[terms.length-1]);
			context.write(ngram, indexitem);
			
			ngram.set(trigram,terms[terms.length-1]);
			context.write(ngram, indexitem);
			
			ngram.set(quadgram,terms[terms.length-1]);
			context.write(ngram, indexitem);
			
		}//end else type base

		
/*
//F1		
		while (i < terms.length - 2) {// -2 coz last token is tf, not needed for docs
			if (terms[i].isEmpty()) { // skip the "" tokens
				i++;
				continue;
			} else {
				int j = i + 1;//tail
				while (j < terms.length && terms[j].isEmpty()) { 
					j++;
				}
				if (j > terms.length - 1) {
					return;
				} else {
					if(indexType==0){//type bigram
						indexitem.set(key.toString(),1);//key from mapper input
						
						//tx bi, tri, quad; lots of network activity
						ngram.set(terms[i], "*");
						context.write(ngram, indexitem);
						// set ngram key
						ngram.set(terms[i], terms[j]);
						// set docid
						context.write(ngram, indexitem);
					}else if(indexType == 1 ){//type base
						ngram.set("*", "*");
						context.write(ngram, indexitem);
						ngram.set(terms[i], "*");
						context.write(ngram, indexitem);
						
					}					
					i = j;
				
				}//else stil have last token
			}//end else terms[] not empty
		}//end while
*/
		
		
	}//end map
}//end mapper
