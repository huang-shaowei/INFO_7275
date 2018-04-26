import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class DelayTimeAverageTuple implements Writable{
	
	private float count = 0;
	private float average = 0;
	private float arrDelay = 0;
	private float depDelay = 0;
	
	public float getCount() {
		return count;
	}
	
	public void setCount(float count) {
		this.count = count;
	}
	
	public float getAverage() {
		return average;
	}
	
	public void setAverage(float average) {
		this.average = average;
	}
	
	public float getArrDelay() {
		return arrDelay;
	}
	
	public void setArrDelay(float arrDelay) {
		this.arrDelay = arrDelay;
	}
	
	public float getDepDelay() {
		return depDelay;
	}
	
	public void setDepDelay(float depDelay) {
		this.depDelay = depDelay;
	}
	
	public void write(DataOutput out) throws IOException {
		out.writeFloat(count);
		out.writeFloat(average);
		out.writeFloat(arrDelay);
		out.writeFloat(depDelay);
		
	}

	public void readFields(DataInput in) throws IOException {
		count = in.readFloat();
		average = in.readFloat();	
		arrDelay = in.readFloat();
		depDelay = in.readFloat();
	}
	
	public String toString() {
		return "," +   arrDelay + "," + depDelay + "," + average;
	}
	
}
