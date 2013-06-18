package ind;

import io.PairOfStringInt;
import io.PairOfStrings;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * @author shin
 * This is the mapper used for building inverted index 
 * for each ngram <code>emit(bigram , {docid , 1 })</code> 
 */
public class IndexMapper extends
		Mapper<Object, Text, PairOfStrings,PairOfStringInt> {

	private static PairOfStrings bigram = new PairOfStrings();
	private static PairOfStringInt indexitem = new PairOfStringInt();
	private static int indexType ;

	
	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		indexType = context.getConfiguration().getInt("indextype",-1);
		
	}

	@Override
	protected void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {
		// Using regular expression to tokenize sentences.
		String terms[] = value.toString().split("[^a-zA-Z0-9']");
		if (terms.length < 2) {
			return;
		}
		int i = 0;
		while (i < terms.length - 1) {
			if (terms[i].isEmpty()) { // skip the "" tokens
				i++;
				continue;
			} else {
				int j = i + 1;
				while (j < terms.length && terms[j].isEmpty()) { // find next
																	// unempty
																	// token
					j++;
				}
				if (j > terms.length - 1) {
					return;
				} else {
					if(indexType==0){
						indexitem.set(key.toString(),1);
						bigram.set(terms[i], "*");
						context.write(bigram, indexitem);
						// set bigram key
						bigram.set(terms[i], terms[j]);
						// set docid
						
						context.write(bigram, indexitem);
					}else if(indexType == 1 ){
						bigram.set("*", "*");
						context.write(bigram, indexitem);
						bigram.set(terms[i], "*");
						context.write(bigram, indexitem);
						
					}
					
					i = j;
					
					
				}
			}

		}

	}
}
