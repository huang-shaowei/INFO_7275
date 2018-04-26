package part3;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

class MedianStdDevReducer extends Reducer<Text, FloatWritable, Text, MedianStdDevTuple>{
	
	private MedianStdDevTuple result = new MedianStdDevTuple();
	private ArrayList<Float> ratingList = new ArrayList<Float>();
	
	public void reduce(Text key, Iterable<FloatWritable> values, Context context) 
			throws IOException, InterruptedException{
		
		float sum = 0;
		float count = 0;
		ratingList.clear();
		result.setStdDev(0);
		
		for (FloatWritable val : values) {
			ratingList.add(val.get());
			sum += val.get();
			++count;			
		}
		
		Collections.sort(ratingList);
		
		// Median
		if(count % 2 == 0) {
			result.setMedian((ratingList.get((int)count / 2 - 1) + 
					ratingList.get((int) count / 2)) / 2.0f);
			
		} else {
			result.setMedian(ratingList.get((int) count / 2));
			
		}
		
		
		// Standard Deviation
		float mean = sum / count;
		float sumOfSquares = 0.0f;
		for(Float f : ratingList) {
			sumOfSquares += (f - mean) * (f - mean);
		}
		result.setStdDev((float) Math.sqrt(sumOfSquares / (count - 1)));
		
		context.write(key, result);
	}
	
}
