package nyse;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class NyseMapper extends Mapper<LongWritable, Text, Text, FloatWritable> {

		private final static FloatWritable price = new FloatWritable();
	       
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
	               
	           
	            String[] fields = value.toString().split(",");
	            if(fields[0].contains("NYSE") && (fields.length>4)){
	                Text t1 = new Text(fields[1]);
	                Float stockPrice = Float.parseFloat(fields[4]);
	                price.set(stockPrice);
	                context.write(t1, price);
	            }
		
	        }
}