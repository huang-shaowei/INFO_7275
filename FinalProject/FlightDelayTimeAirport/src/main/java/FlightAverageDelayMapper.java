import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class FlightAverageDelayMapper extends Mapper<Object, Text, Text, DelayTimeAverageTuple> {
		
		private Text airport = new Text();
		private DelayTimeAverageTuple outDelay = new DelayTimeAverageTuple();
	       
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
	                          
	            String[] fields = value.toString().split(",");
	            try {
	            	String origin = fields[16];
	            	String dest = fields[17];
	            	int ArrDelay = Integer.parseInt(fields[14]);
		            int DepDelay = Integer.parseInt(fields[15]);
		            
		            airport.set(origin);
		            outDelay.setArrDelay(ArrDelay);
		            outDelay.setDepDelay(DepDelay);
		            outDelay.setCount(1);
		            
		            context.write(airport, outDelay);
	            	
	            } catch (Exception e) {
	            	
	            }
		
	    }
}