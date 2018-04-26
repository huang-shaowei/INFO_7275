import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class DelayRankingMapper extends Mapper<Object, Text, DoubleWritable, Text>{
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		
		String[] fields = value.toString().split(":");

		try {
			
			DoubleWritable ratio = new DoubleWritable(Double.parseDouble(fields[1]));
			Text airport = new Text(fields[0]);
			context.write(ratio, airport);
			
		} catch (Exception e) {
			
		}
		
	
}

}
