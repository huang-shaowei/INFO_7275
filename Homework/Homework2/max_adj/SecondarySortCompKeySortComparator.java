package max_adj;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class SecondarySortCompKeySortComparator extends WritableComparator {

  protected SecondarySortCompKeySortComparator() {
		super(CompositeKeyWritable.class, true);
	}

	@Override
	public int compare(WritableComparable w1, WritableComparable w2) {
		CompositeKeyWritable key1 = (CompositeKeyWritable) w1;
		CompositeKeyWritable key2 = (CompositeKeyWritable) w2;

		int cmpResult = key1.getSymbol().compareTo(key2.getSymbol());
		if (cmpResult == 0)// same deptNo
		{
			return -key1.getMaxAdj()
					.compareTo(key2.getMaxAdj());
			//If the minus is taken out, the values will be in
			//ascending order
		}
		return cmpResult;
	}
}