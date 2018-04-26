import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

class DistinctRouteReducer extends Reducer<OriginDestTuple, NullWritable, OriginDestTuple, NullWritable>{

	public void reduce(OriginDestTuple key, Iterable<NullWritable> values,Context context)
			throws IOException, InterruptedException {
		
		context.write(key, NullWritable.get());
	}

	
}
