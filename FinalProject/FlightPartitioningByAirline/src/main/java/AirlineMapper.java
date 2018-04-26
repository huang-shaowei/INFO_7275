import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

class AirlineMapper extends Mapper<Object, Text, Text, Text>{
	
	private Text outkey = new Text();
		
	protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		
		String[] fields = value.toString().split(",");
		
		try {
						
			outkey.set(fields[8]);
			
			context.write(outkey, value);
			
		} catch (Exception e) {
			
		}
		
	}
	
}
