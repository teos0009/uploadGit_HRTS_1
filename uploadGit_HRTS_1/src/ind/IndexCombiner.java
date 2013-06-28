package ind;

import io.PairOfStringInt;
import io.PairOfStrings;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;


import org.apache.hadoop.mapreduce.Reducer;


/**
 * @author shin 
 * This is the combiner used for building ngram inverted index
 * the tf of bigram in same document is summed in this combiner 
 * for each bigram <code>emit(bigram , {docid,tf})</code>  to the reducer
 */
public class IndexCombiner extends
		Reducer<PairOfStrings,PairOfStringInt, PairOfStrings, PairOfStringInt> {
    private static PairOfStringInt indexitem= new PairOfStringInt();
	
	@Override
	protected void reduce(PairOfStrings key, Iterable<PairOfStringInt> value, Context context)
			throws IOException, InterruptedException {
		
		Map<String,Integer> indexitems = new HashMap<String,Integer>();
		for(PairOfStringInt v : value){
			if(!indexitems.containsKey(v.getLeftElement())){
				indexitems.put(v.getLeftElement(), 1);
			}else{
				//shin: this case not needed
				//int tf = indexitems.get(v.getLeftElement());
				//indexitems.put(v.getLeftElement(), tf+1);
			}
			
		}
		for(Entry<String, Integer> e : indexitems.entrySet()){
			indexitem.set(e.getKey(), e.getValue());
			context.write(key, indexitem);
		}
		
		
	}

}
