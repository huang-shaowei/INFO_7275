package nyse;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class NyseReducer extends Reducer<Text, FloatWritable, Text, FloatWritable> {

        private FloatWritable avg = new FloatWritable();

       
	public void reduce(Text key, Iterable<FloatWritable> values,Context context)
			throws IOException, InterruptedException {
		Float sum = 0.f ;
	    int count = 0;
		
		for (FloatWritable val : values) {
			sum +=val.get();
			count++;
		}   
			
			avg.set(sum/count);
		context.write(key, avg);
	}



}