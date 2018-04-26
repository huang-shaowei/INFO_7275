import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class DelayAirportMapper extends Mapper<LongWritable, Text, Text, DelayCountRatioTuple> {

		private DelayCountRatioTuple outCountRatio = new DelayCountRatioTuple();
		private Text airportOut = new Text();
	       
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
	               
	           
			String[] fields = value.toString().split(",");
            try {
            	
            	int ArrDelay = Integer.parseInt(fields[14]);
	            int DepDelay = Integer.parseInt(fields[15]);
	            
	            String airport = fields[16];
	            
	            airportOut.set(airport);
	            outCountRatio.setTotalCount(1);
	            
	            if(ArrDelay > 0 || DepDelay > 0) {	            	
	            	outCountRatio.setDelayCount(1);	            	
	            }
	            
	            else {
	            	outCountRatio.setDelayCount(0);
	            }
	            context.write(airportOut, outCountRatio);
            	
            } catch (Exception e) {
            	
            }
	            
		
	    }
}