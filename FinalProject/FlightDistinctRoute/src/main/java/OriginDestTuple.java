import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

public class OriginDestTuple implements Writable,WritableComparable{

	private String origin = null;
	private String dest = null;
	
	public String getOrigin() {
		return origin;
	}
	
	public void setOrigin(String origin) {
		this.origin = origin;
	}
	
	public String getDest() {
		return dest;
	}
	
	public void setDest(String dest) {
		this.dest = dest;
	}
	
	public void write(DataOutput out) throws IOException {
		out.writeUTF(origin);
		out.writeUTF(dest);		
	}

	public void readFields(DataInput in) throws IOException {
		origin = in.readUTF();
		dest = in.readUTF();
	}
	
	public String toString() {
		return origin + "-" + dest;
	}

	public int compareTo(Object o) {
		OriginDestTuple t = (OriginDestTuple) o;
		int result =origin.compareTo(t.origin);
		if (result==0){
			result=dest.compareTo(t.dest);
		}
		
		return result;
		
	}

	

}
