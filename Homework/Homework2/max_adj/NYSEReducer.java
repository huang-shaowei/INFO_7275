package max_adj;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

import com.sun.tools.javadoc.JavaScriptScanner.Reporter;


public class NYSEReducer extends 
		Reducer<CompositeKeyWritable, NullWritable, CompositeKeyWritable, NullWritable> {
	
		
		
		public void reduce(CompositeKeyWritable key, Iterable<NullWritable> values, Context context)
	            throws IOException, InterruptedException {
			
			
			
			int count =0;
			for(NullWritable val: values ) {
				
				if(count<1) {
					context.write(key,NullWritable.get());		
				
					count++;
				}
				
				
				
			}
			
			
			
		}
}
