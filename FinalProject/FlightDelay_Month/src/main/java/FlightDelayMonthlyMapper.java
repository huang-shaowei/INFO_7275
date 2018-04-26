import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class FlightDelayMonthlyMapper extends Mapper<LongWritable, Text, IntWritable, DelayCountRatioTuple> {

		private DelayCountRatioTuple outCountRatio = new DelayCountRatioTuple();
		private IntWritable monthOut = new IntWritable();
	       
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
	               
	           
			String[] fields = value.toString().split(",");
            try {
            	
            	int ArrDelay = Integer.parseInt(fields[14]);
	            int DepDelay = Integer.parseInt(fields[15]);
	            
	            int month = Integer.parseInt(fields[1]);
	            
	            monthOut.set(month);
	            outCountRatio.setTotalCount(1);
	            
	            if(ArrDelay > 0 || DepDelay > 0) {	            	
	            	outCountRatio.setDelayCount(1);	            	
	            }
	            
	            else {
	            	outCountRatio.setDelayCount(0);
	            }
	            context.write(monthOut, outCountRatio);
            	
            } catch (Exception e) {
            	
            }
	            
		
	    }
}