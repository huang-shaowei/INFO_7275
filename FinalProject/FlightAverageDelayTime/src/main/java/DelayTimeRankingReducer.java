import java.io.IOException;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class DelayTimeRankingReducer extends Reducer <DoubleWritable, Text,Text, DoubleWritable>{
	
	private int nCount = 0;
	protected void setup(Context context) throws IOException, InterruptedException {
		nCount=0;
	}
	@Override
	public void reduce(DoubleWritable key, Iterable<Text> values, Context context) {
		
			try {
				
				for (Text val :values) {
					
					if (nCount < 100) {
						context.write(val, key);
					}
					else return;
					nCount++;
				}
				
			} catch (Exception e) {
				
			}
		
	}
}
