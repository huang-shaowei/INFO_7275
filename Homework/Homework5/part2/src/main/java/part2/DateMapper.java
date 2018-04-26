package part2;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

class DateMapper extends Mapper<Object, Text, IntWritable, Text>{
	//14/Jul/2015:15:29:35
	private final static SimpleDateFormat frmt = new SimpleDateFormat("dd/MMM/yyyy");
	
	private IntWritable outkey = new IntWritable();
	
	
	protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		
		String[] fields = value.toString().split(" ");
		
		try {
			
			String strDate="";
			
			for(int i=0; i<fields.length; i++) {
				if(fields[i].contains("[")) {
					strDate = fields[i].substring(1,12);
					break;
				}
			}
			
			
			Date accessDate = frmt.parse(strDate);
						
			Calendar calendar = new GregorianCalendar();
			calendar.setTime(accessDate);
			System.out.println(strDate + "\t" + accessDate + "\t" + calendar.get(Calendar.MONTH));
			outkey.set(calendar.get(Calendar.MONTH));
			
			
			context.write(outkey, value);
			
		} catch (Exception e) {
			
		}
		
	}
	
}
