package io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

/**
 * @author Tianyi Xiong
 * The io class used for building invertedindex which contains,termfreq,docfreq 
 * probability and List of (docid,tf) of the bigram
 * This class can also used as the data class to store inverted index information
 * after being read from the output file in sequence format.
 */
public class InvertedIndex implements Writable {

	public static final int TYPE_BIGRAM = 0;
	public static final int TYPE_BASE = 1;
	private int termfreq;
	private int type;
	private ArrayListWritable<PairOfStringInt> indexlist;

	public InvertedIndex() {
		
	}

	/**
	 * @return tf of this bigram
	 */
	public int getTermfreq() {
		return termfreq;
	}
	
	public void setTermfreq(int termfreq) {
		this.termfreq = termfreq;
	}

	public int getDocfreq() {
		return indexlist.size();
	}

	

	

	public ArrayListWritable<PairOfStringInt> getIndexList() {
		return indexlist;
	}

	public void setIndex(ArrayListWritable<PairOfStringInt> indexlist) {
		this.indexlist = indexlist;
	}

	/**
	 * Deserializes this object.
	 * 
	 * @param in
	 *            source for raw byte representation
	 */
	@Override
	public void readFields(DataInput in) throws IOException {

		termfreq = in.readInt();
		type = in.readInt();
		if(type==TYPE_BIGRAM){
			indexlist = new ArrayListWritable<PairOfStringInt>();
			indexlist.readFields(in);
		}
		
		
	}

	/**
	 * Serializes this object.
	 * 
	 * @param out
	 *            where to write the raw byte representation
	 */
	@Override
	public void write(DataOutput out) throws IOException {

		out.writeInt(termfreq);
		out.writeInt(type);
		if(type==TYPE_BIGRAM){
			indexlist.write(out);
		}
		

	}

	@Override
	public String toString() {
		StringBuilder s = new StringBuilder();

		s.append("{");
		s.append(termfreq);
		s.append(" ");
		
		

		if (indexlist == null) {
			s.append("{}");
		} else {
			s.append("{");
			for (int i = 0; i < indexlist.size(); i++) {
				s.append(indexlist.get(i));
				if (i < indexlist.size() - 1)
					s.append(", ");
			}
			s.append("}");
		}

		s.append(" }");

		return s.toString();
	}

	public int getType() {
		return type;
	}

	public void setType(int type) {
		this.type = type;
	}

	

}
