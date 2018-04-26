package part2;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

class RatingJoinMapper extends Mapper<Object, Text, Text, Text>{
	
	private Text outkey = new Text();
	private Text outvalue = new Text();
	
	public void map(Object key, Text value, Context context) 
			throws IOException, InterruptedException {
		
		String[] fields = value.toString().split(";");
		try {
			
			outkey.set(fields[0]);
			outvalue.set("R" + value.toString());
			context.write(outkey, outvalue);
		
		} catch (Exception e) {
			System.out.println(e);
		}
	}
}