import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

class FlightDelayFilterMapper extends Mapper<Object, Text, Text, NullWritable>{
	
	private Text result = new Text();
	
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		
		String fields[] = value.toString().split(",");
		
		try {
			int ArrDelay = Integer.parseInt(fields[14]);
            int DepDelay = Integer.parseInt(fields[15]);
			
            if(ArrDelay > 0 && DepDelay > 0) {	            	
            	result.set(value);  
            	context.write(result, NullWritable.get());
            }
			
			
		} catch (Exception e) {
			
		}
  
	}
	
}
