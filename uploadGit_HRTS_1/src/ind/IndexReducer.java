package ind;

import io.ArrayListWritable;
import io.InvertedIndex;
import io.PairOfStringInt;
import io.PairOfStrings;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.mapreduce.Reducer;


/**
 * @author shin
 *         This is the reducer used for building bigram inverted
 *         index. For each ngram , <code>emit(ngram, invertedindex)</code> and
 *         the List of (docid,tf) is sorted. The count of bigram which start
 *         with word w is stored in an temporary folder in order to calculate
 *         probability of each bigram in anther MapReduce job.
 */
public class IndexReducer extends
		Reducer<PairOfStrings, PairOfStringInt, PairOfStrings, InvertedIndex> {

	private static InvertedIndex invertedindex = new InvertedIndex();

	private static int indexType;

	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		indexType = context.getConfiguration().getInt("indextype", -1);

	}

	@Override
	//key is (ngram string, tf), index is docid, count=1
	//map reduce pattern used: secondary sorting style
	protected void reduce(PairOfStrings key, Iterable<PairOfStringInt> value,
			Context context) throws IOException, InterruptedException {
		int termfreq = 0;
		if (indexType == InvertedIndex.TYPE_BASE) { // if is a (aaa,*) bigram
			for (PairOfStringInt v : value) {
				termfreq += v.getRightElement();
			}
			invertedindex.setTermfreq(termfreq);
			invertedindex.setType(indexType);
			// don't generate indexlist
			invertedindex.setIndex(null);
			context.write(key, invertedindex);//shin: this case key is (ngram string, tf); no index

		} 
		//build the inverted index for key is (ngram,tf), value is list of docid
		else if(indexType == InvertedIndex.TYPE_BIGRAM){
			Map<String, Integer> indexitems = new HashMap<String, Integer>();
			for (PairOfStringInt v : value) {
				if (!indexitems.containsKey(v.getLeftElement())) {//shin: left is docid, right is count=1
					indexitems.put(v.getLeftElement(), v.getRightElement());
				} else {
					//shin: not needed for this use case
					//int tf = indexitems.get(v.getLeftElement());
					//indexitems.put(v.getLeftElement(), tf + v.getRightElement());
				}
				//termfreq += v.getRightElement();

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
		}

	}

}
