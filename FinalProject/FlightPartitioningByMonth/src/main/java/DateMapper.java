import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

class DateMapper extends Mapper<Object, Text, IntWritable, Text>{
	
	private IntWritable outkey = new IntWritable();
		
	protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		
		String[] fields = value.toString().split(",");
		
		try {
			
						
			outkey.set(Integer.parseInt(fields[1]));
			
			
			context.write(outkey, value);
			
		} catch (Exception e) {
			
		}
		
	}
	
}
