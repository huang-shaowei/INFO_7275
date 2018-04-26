package part4;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

class MedianStdDevMapper extends Mapper<Object, Text, Text, SortedMapWritable>{
	
	private static final LongWritable ONE = new LongWritable(1);
	private Text outMovie = new Text();
	private FloatWritable rating = new FloatWritable();
	
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		
		String[] fields = value.toString().split("::");
		try {
			
			outMovie.set(fields[1]);
			rating.set(Float.parseFloat(fields[2]));
			
			SortedMapWritable outRating = new SortedMapWritable();
			//outRating.setRating(rating.get());
			//outRating.setCount(ONE.get());
			outRating.put(rating,ONE);
			
			
			
			context.write(outMovie, outRating);
			
		} catch (Exception e) {
			
		}
		
	}
}
