import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class DelayCountRatioTuple implements Writable{
	private float totalCount = 0;
	private float delayCount = 0;
	private float ratio = 0; 
	
	public float getTotalCount() {
		return totalCount;
	}
	
	public void setTotalCount(float totalCount) {
		this.totalCount = totalCount;
	}
	
	public float getDelayCount() {
		return delayCount;
	}
	
	public void setDelayCount(float delayCount) {
		this.delayCount = delayCount;
	}
	
	public float getRatio() {
		return ratio;
	}
	
	public void setRatio(float ratio) {
		this.ratio = ratio;
	}
	
	public void write(DataOutput out) throws IOException {
		out.writeFloat(totalCount);
		out.writeFloat(delayCount);
		out.writeFloat(ratio);
		
	}

	public void readFields(DataInput in) throws IOException {
		totalCount = in.readFloat();
		delayCount= in.readFloat();	
		ratio = in.readFloat();
	}
	
	public String toString() {
		return "" + ":"+ ratio;
	}
}
