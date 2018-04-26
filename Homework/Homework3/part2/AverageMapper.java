import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;



public class AverageMapper extends 
	Mapper<Object, Text, IntWritable, CountAverageTuple>{
	
	private IntWritable outYear = new IntWritable();
	private CountAverageTuple outCountAverage = new CountAverageTuple();
	
	private SimpleDateFormat frmt = new SimpleDateFormat("yyyy-mm-dd");
	
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		
		String fields[] = value.toString().split(",");
		
		try {
			if((fields[0].equals("NYSE")) && (fields.length > 8)) {
				
				String strDate = fields[2];
				Float adj = Float.parseFloat(fields[8]);
				
				Date stockDate = frmt.parse(strDate);
				Calendar calendar = new GregorianCalendar();
				calendar.setTime(stockDate);
				outYear.set(calendar.get(Calendar.YEAR));
				
				
				outCountAverage.setCount(1);
				outCountAverage.setAverage(adj);
				
				context.write(outYear, outCountAverage);
				
			}
			
		} catch (Exception e) {
			
		}
		
		
	}
}
