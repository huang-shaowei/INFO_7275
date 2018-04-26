import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

class DelayTimeInvertedIndexMapper extends Mapper<Object, Text, Text, Text>{
	
	private Text route = new Text();
	private Text delayTimeRange = new Text();
	
	public void map(Object key, Text value, Context context) 
			throws IOException, InterruptedException {
		
		String[] fields = value.toString().split(",");
		
		try {
				
				route.set(fields[0].trim());			
				float time = Float.parseFloat(fields[3]);
				System.out.println(time);
				
				if(time >= 0 && time < 15) {
					delayTimeRange.set("Average Delay Time Below 15 mins: ");
				}
				else if (time >= 15 && time < 30) {
					delayTimeRange.set("Average Delay Time Between 15 to 30 mins: ");
				}
				else if (time >= 30 && time < 45) {
					delayTimeRange.set("Average Delay Time Between 30 to 45 mins: ");
				}
				else if (time >= 45 && time < 60) {
					delayTimeRange.set("Average Delay Time Between 45 to 60 mins: ");
				}
				else if (time >= 60 && time < 75) {
					delayTimeRange.set("Average Delay Time Between 60 to 75 mins: ");
				}
				else if (time >= 75 && time < 90) {
					delayTimeRange.set("Average Delay Time Between 75 to 90 mins: ");
				}
				else if (time >= 90 && time < 120) {
					delayTimeRange.set("Average Delay Time Between 90 to 120 mins: ");
				}
				else {
					delayTimeRange.set("Average Delay Time Over 2 hours: ");
				}
			
			context.write(delayTimeRange, route);
		} catch (Exception e) {
			
		}
	}
}
