package part2;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

class URBJoinReducer extends Reducer<Text, Text, Text, Text>{
	
	private ArrayList<Text> listUR = new ArrayList<Text>();
	private ArrayList<Text> listB = new ArrayList<Text>();
	
	public void reduce(Text key, Iterable<Text> values, Context context) 
			throws IOException, InterruptedException {
		
		listUR.clear();
		listB.clear();
		
		for (Text val : values) {
			if(val.charAt(0) == '2') {
				listUR.add(new Text(val.toString().substring(2)));
			}
			else if (val.charAt(0) == 'B') {
				listB.add(new Text(val.toString().substring(1)));
			}
		}
		
		InnerJoin(context);
	}
	
	private void InnerJoin(Context context) 
			throws IOException, InterruptedException {
		if (!listUR.isEmpty() && !listB.isEmpty()) {
			for (Text U : listUR) {
				for (Text R : listB) {
					context.write(U, R);
				}
			}
		}
	}
}
