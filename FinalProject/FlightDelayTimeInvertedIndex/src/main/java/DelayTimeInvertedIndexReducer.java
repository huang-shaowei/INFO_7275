import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

class DelayTimeInvertedIndexReducer extends Reducer<Text, Text, Text, Text> {
	
	private Text result = new Text();
	
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		
		StringBuilder routeList = new StringBuilder();
		
		for(Text val : values) {
			routeList.append(val).append(",");
		}
		
		result.set(routeList.toString());
		context.write(key, result);
		
	}
}
