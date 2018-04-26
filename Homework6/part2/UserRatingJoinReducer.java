package part2;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

class UserRatingJoinReducer extends Reducer<Text, Text, Text, Text>{
	
	private ArrayList<Text> listU = new ArrayList<Text>();
	private ArrayList<Text> listR = new ArrayList<Text>();
	
	public void reduce(Text key, Iterable<Text> values, Context context) 
			throws IOException, InterruptedException {
		
		listU.clear();
		listR.clear();
		
		for (Text val : values) {
			if(val.charAt(0) == 'U') {
				listU.add(new Text(val.toString().substring(1)));
			}
			else if (val.charAt(0) == 'R') {
				listR.add(new Text(val.toString().substring(1)));
			}
		}
		
		InnerJoin(context);
	}
	
	private void InnerJoin(Context context) 
			throws IOException, InterruptedException {
		if (!listU.isEmpty() && !listR.isEmpty()) {
			for (Text U : listU) {
				for (Text R : listR) {
					context.write(U, R);
				}
			}
		}
	}
}
