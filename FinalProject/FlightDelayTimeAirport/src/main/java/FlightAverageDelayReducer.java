import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class FlightAverageDelayReducer extends Reducer<Text, DelayTimeAverageTuple, Text, DelayTimeAverageTuple> {

       
    private DelayTimeAverageTuple result = new DelayTimeAverageTuple();

       
	public void reduce(Text key, Iterable<DelayTimeAverageTuple> values,Context context)
			throws IOException, InterruptedException {
		
		float sum = 0;
		float arrDelay = 0;
		float depDelay = 0;
		float ration = 0;
		
		for (DelayTimeAverageTuple val : values) {
			sum += val.getCount();
			arrDelay += val.getArrDelay();
			depDelay += val.getDepDelay();
		}   
		
		result.setDepDelay(depDelay / sum);
		result.setArrDelay(arrDelay / sum);
		result.setAverage((depDelay + arrDelay) / sum);
			
		context.write(key, result);
	}



}