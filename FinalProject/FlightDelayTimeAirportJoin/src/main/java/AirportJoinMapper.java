import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

class AirportJoinMapper extends Mapper<Object, Text, Text, Text>{
	
	private Text outkey = new Text();
	private Text outvalue = new Text();
	
	public void map(Object key, Text value, Context context) 
			throws IOException, InterruptedException {
		
		String[] fields = value.toString().split(",");
		try {
			
			outkey.set(fields[0].substring(1, 4));
			outvalue.set("A" + fields[1] + "," + fields[2] + "," + fields[3]);
			context.write(outkey, outvalue);
		
		} catch (Exception e) {
			System.out.println(e);
		}
	}
}
