package max_adj;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class NYSEMapper extends Mapper<Object, Text, CompositeKeyWritable, NullWritable> {
	
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

		String values[] = value.toString().split(",");
		
		
		String symbol = null;
		Double max_adj = (double)0;
		
		
		try {
			if(values[0].contains("NYSE") && (values.length>8)) {
				
				symbol = values[1];
				System.out.println(values[8]);
				max_adj = Double.parseDouble(values[8]);
				
				
			
			}
			
		} catch (Exception e) {
			// TODO: handle exception
		}
		


		if (null != symbol || null != max_adj ) {
			
			CompositeKeyWritable mapout = new CompositeKeyWritable(symbol,max_adj);
			
			try {
				System.out.println(symbol + " " + max_adj );
				context.write(mapout,NullWritable.get());
			} catch (Exception e) {
				
				System.out.println("" + e.getMessage());

			}

		}

	}

}
