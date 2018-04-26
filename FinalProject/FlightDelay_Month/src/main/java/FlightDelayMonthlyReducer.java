import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class FlightDelayMonthlyReducer extends Reducer<IntWritable, DelayCountRatioTuple, IntWritable, DelayCountRatioTuple> {

    private DelayCountRatioTuple result = new DelayCountRatioTuple();

       
	public void reduce(IntWritable key, Iterable<DelayCountRatioTuple> values,Context context)
			throws IOException, InterruptedException {
		
		float sum = 0;
		float delay = 0;
		float ratio = 0;
		
		for (DelayCountRatioTuple val : values) {
			sum += val.getTotalCount();
			delay += val.getDelayCount();
		}   
		result.setTotalCount(sum);
		result.setDelayCount(delay);
		result.setRatio(delay / sum);
			
		context.write(key, result);
	}



}