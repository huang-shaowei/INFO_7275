package part4;

import java.io.IOException;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Reducer;

class MedianStdDevReducer extends 
Reducer<Text, SortedMapWritable, Text, MedianStdDevTuple> {
	
	private MedianStdDevTuple result = new MedianStdDevTuple();
	private TreeMap<Float, Long> ratingCounts = 
			new TreeMap<Float, Long>();
	
	public void reduce(Text key, Iterable<SortedMapWritable> values,
			Context context) throws IOException, InterruptedException {
		
		
		float sum = 0;
		long totalRatings = 0;
		ratingCounts.clear();
		result.setMedian(0);
		result.setStdDev(0);
		
		for (SortedMapWritable v : values) {
			for(Entry<WritableComparable, Writable> entry : v.entrySet()) {
				float rate = ((FloatWritable) entry.getKey()).get();
				long count = ((LongWritable) entry.getValue()).get();
				totalRatings += count;
				sum += rate * count;
				
				Long storedCount = ratingCounts.get(rate);
				if(storedCount == null) {
					ratingCounts.put(rate, count);
				} else {
					ratingCounts.put(rate, storedCount + count);
				}			
			}		
		}
		
		long medianIndex = totalRatings / 2L;
		long previousRatings = 0;
		long ratings = 0;
		float prevKey = 0;
		for(Entry<Float, Long> entry : ratingCounts.entrySet()) {
			ratings = previousRatings + entry.getValue();
			
			if (previousRatings <= medianIndex && medianIndex < ratings) {
				if (totalRatings % 2 == 0 && previousRatings == medianIndex) {
					result.setMedian((float) (entry.getKey() + prevKey) / 2.0f);
				} else {
					result.setMedian(entry.getKey());
				}
				break;
			}
			
			previousRatings = ratings;
			prevKey = entry.getKey();
		}
		
		float mean = sum / totalRatings;
		
		float sumOfSquares = 0.0f;
		for(Entry<Float, Long> entry : ratingCounts.entrySet()) {
			sumOfSquares += (entry.getKey() - mean) * (entry.getKey() - mean) *entry.getValue();
		}
		result.setStdDev((float) Math.sqrt(sumOfSquares / (totalRatings - 1)));
		context.write(key, result);
	}
}
