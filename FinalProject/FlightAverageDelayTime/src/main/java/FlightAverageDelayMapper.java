import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class FlightAverageDelayMapper extends Mapper<Object, Text, OriginDestTuple, DelayTimeAverageTuple> {
		
		private OriginDestTuple route = new OriginDestTuple();
		private DelayTimeAverageTuple outDelay = new DelayTimeAverageTuple();
	       
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
	                          
	            String[] fields = value.toString().split(",");
	            try {
	            	String origin = fields[16];
	            	String dest = fields[17];
	            	int ArrDelay = Integer.parseInt(fields[14]);
		            int DepDelay = Integer.parseInt(fields[15]);
		            
		            route.setOrigin(origin);
		            route.setDest(dest);
		            outDelay.setArrDelay(ArrDelay);
		            outDelay.setDepDelay(DepDelay);
		            outDelay.setCount(1);
		            
		            context.write(route, outDelay);
	            	
	            } catch (Exception e) {
	            	
	            }
		
	    }
}