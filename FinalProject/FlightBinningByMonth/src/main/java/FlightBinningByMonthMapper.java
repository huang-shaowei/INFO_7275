import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

public class FlightBinningByMonthMapper extends Mapper<Object,Text,Text, NullWritable> { 
	
	private MultipleOutputs<Text,NullWritable> mos = null;
	
	protected void setup(Context context) {
		mos = new MultipleOutputs(context);
	}
	
	protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		
		String[] fields = value.toString().split(",");
		
		try {
			
			String month = fields[1];
			
			if(month.equalsIgnoreCase("1")) {
				mos.write("bins", value, NullWritable.get(), "JAN-flight");
			}
			if(month.equalsIgnoreCase("2")) {
				mos.write("bins", value, NullWritable.get(), "FEB-flight");
			}
			if(month.equalsIgnoreCase("3")) {
				mos.write("bins", value, NullWritable.get(), "MAR-flight");
			}
			if(month.equalsIgnoreCase("4")) {
				mos.write("bins", value, NullWritable.get(), "APR-flight");
			}
			if(month.equalsIgnoreCase("5")) {
				mos.write("bins", value, NullWritable.get(), "MAY-flight");
			}
			if(month.equalsIgnoreCase("6")) {
				mos.write("bins", value, NullWritable.get(), "JUN-flight");
			}
			if(month.equalsIgnoreCase("7")) {
				mos.write("bins", value, NullWritable.get(), "JUL-flight");
			}
			if(month.equalsIgnoreCase("8")) {
				mos.write("bins", value, NullWritable.get(), "AUG-flight");
			}
			if(month.equalsIgnoreCase("9")) {
				mos.write("bins", value, NullWritable.get(), "SEP-flight");
			}
			if(month.equalsIgnoreCase("10")) {
				mos.write("bins", value, NullWritable.get(), "OCT-flight");
			}
			if(month.equalsIgnoreCase("11")) {
				mos.write("bins", value, NullWritable.get(), "NOV-flight");
			}
			if(month.equalsIgnoreCase("12")) {
				mos.write("bins", value, NullWritable.get(), "DEC-flight");
			}
			
		} catch (Exception e) {
			
		}
	}
	
	protected void cleanup(Context context) throws IOException, InterruptedException{
		mos.close();
	}
}
