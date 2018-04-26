package part3;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

class MedianStdDevMapper extends Mapper<Object, Text, Text, FloatWritable>{
	
	private Text outMovie = new Text();
	private FloatWritable outRating = new FloatWritable();
	
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
		
		String[] fields = value.toString().split("::");
		
		try {
			outMovie.set(fields[1]);
			outRating.set(Float.parseFloat(fields[2]));
			
			context.write(outMovie, outRating);
			
		} catch (Exception e) {
			
		}
		
	}
}
