import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

class FlightAirportJoinReducer extends Reducer<Text, Text, Text, Text>{
	
	private ArrayList<Text> listA = new ArrayList<Text>();
	private ArrayList<Text> listF = new ArrayList<Text>();
	
	public void reduce(Text key, Iterable<Text> values, Context context) 
			throws IOException, InterruptedException {
		
		listA.clear();
		listF.clear();
		
		for (Text val : values) {
			if(val.charAt(0) == 'A') {
				listA.add(new Text(val.toString().substring(1)));
			}
			else if (val.charAt(0) == 'F') {
				listF.add(new Text(val.toString().substring(1)));
			}
		}
		
		InnerJoin(context);
	}
	
	private void InnerJoin(Context context) 
			throws IOException, InterruptedException {
		if (!listA.isEmpty() && !listF.isEmpty()) {
			for (Text F : listF) {
				for (Text A : listA) {
					context.write(F, A);
				}
			}
		}
	}
}
