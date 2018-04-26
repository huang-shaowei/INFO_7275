package part3;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

public class BinningMapper extends Mapper<Object,Text,Text, NullWritable> { 
	
	private MultipleOutputs<Text,NullWritable> mos = null;
	
	protected void setup(Context context) {
		mos = new MultipleOutputs(context);
	}
	
	protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		
		String[] fields = value.toString().split(" ");
		
		try {
			
			String request = fields[5].substring(1);
			
			if(request.equalsIgnoreCase("GET")) {
				mos.write("bins", value, NullWritable.get(), "GET-request");
			}
			if(request.equalsIgnoreCase("POST")) {
				mos.write("bins", value, NullWritable.get(), "POST-request");
			}
			
		} catch (Exception e) {
			
		}
	}
	
	protected void cleanup(Context context) throws IOException, InterruptedException{
		mos.close();
	}
}
