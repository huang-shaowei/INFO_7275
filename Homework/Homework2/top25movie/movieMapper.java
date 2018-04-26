package top25movie;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;



class movieMapper extends Mapper <Object, Text, Text, DoubleWritable>{

	private  final static Text movie = new Text();
	private  final static DoubleWritable rating = new DoubleWritable();
	
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		
			String[] fields = value.toString().split("::");
			
			String m = null;
			Double r = null;
			
			try {
				
				m = fields[1];
				r = Double.parseDouble(fields[2]);
				movie.set(m);
				rating.set(r);
				context.write(movie,rating);
				
			} catch (Exception e) {
				
			}
			
		
	}
	
}
