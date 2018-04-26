package part4;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

class MedianStdDevTuple implements Writable{
	
	private float median = 0;
	private float stdDev = 0;
	
	public float getMedian() {
		return median;
	}
	
	public void setMedian(float median) {
		this.median = median;
	}
	
	public float getStdDev() {
		return stdDev;
	}
	
	public void setStdDev(float stdDev) {
		this.stdDev = stdDev;
	}
	
	public void write(DataOutput out) throws IOException {
		out.writeFloat(median);
		out.writeFloat(stdDev);
	}

	public void readFields(DataInput in) throws IOException {
		median = in.readFloat();
		stdDev = in.readFloat();
		
	}
	
	public String toString() {
		return "" + median + "\t" + "" + stdDev;
	}

}
