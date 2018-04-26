
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;



public class DoubleComparator extends WritableComparator{
	
protected DoubleComparator() {
		
		super(DoubleWritable.class,true);
		
	}
	
	public int compare(WritableComparable w1,WritableComparable w2){
		
		return -super.compare(w1, w2);	
	}

	
}
