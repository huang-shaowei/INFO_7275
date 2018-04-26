import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

class DistinctRouteMapper extends Mapper<Object, Text, OriginDestTuple, NullWritable>{
	
	private OriginDestTuple route = new OriginDestTuple();
	
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			
			String fields[] = value.toString().split(",");
			
			try {
				String origin = fields[16];
            	String dest = fields[17];
            	route.setOrigin(origin);
	            route.setDest(dest);
				context.write(route, NullWritable.get());
				
				
			} catch (Exception e) {
				
			}
	}
}