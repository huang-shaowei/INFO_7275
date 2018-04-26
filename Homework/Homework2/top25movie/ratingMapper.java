package top25movie;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;



class ratingMapper extends Mapper <Object, Text, DoubleWritable, Text>{

	private DoubleWritable rating = new DoubleWritable();
	private Text movie = new Text();
	
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		
		String[] fields = value.toString().split("\t");
		
		Double r = null;	
		String m = null;
		 
			
		try {
						
			r = Double.parseDouble(fields[0]);
			m = fields[1];
			
			rating.set(r);
			movie.set(m);
			System.out.println(movie.toString() + ":" + rating.toString());
			context.write(rating,movie);
			
		} catch (Exception e) {
			
		}
		
		
			
		
	}
	
}