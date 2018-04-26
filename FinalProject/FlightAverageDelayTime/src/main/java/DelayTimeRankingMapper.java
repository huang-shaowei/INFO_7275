
import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class DelayTimeRankingMapper extends Mapper<Object, Text, DoubleWritable, Text>{
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		
		String[] fields = value.toString().split(",");

		try {
			
			DoubleWritable delayTime = new DoubleWritable(Double.parseDouble(fields[3]));
			Text route = new Text(fields[0].trim());
			context.write(delayTime, route);
			
		} catch (Exception e) {
			
		}
		
	
	}

}
