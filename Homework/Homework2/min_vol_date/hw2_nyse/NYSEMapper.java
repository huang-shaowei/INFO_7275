package hw2_nyse;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class NYSEMapper extends Mapper<Object, Text, CompositeKeyWritable, NullWritable> {
	/*
	private String symbol;
	private Long max_vol;
	private String max_date;
	private Long min_vol;
	private String min_date;
	private Long price_adj;
	*/
	//private CompositeKeyWritable mapout = new CompositeKeyWritable();
	
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

		String values[] = value.toString().split(",");
		
		
		String symbol = null;
		Long max_vol = (long)0;
		Long min_vol= (long)0;

		String max_date = null;
		String min_date = null;
		
		try {
			if(values[0].contains("NYSE") && (values.length>8)) {
				
				symbol = values[1];
				max_date = values[2];
				min_date = values[2];
				max_vol = Long.parseLong(values[7]);
				min_vol = Long.parseLong(values[7]);
				
			
			}
			
		} catch (Exception e) {
			// TODO: handle exception
		}
		


		if (null != symbol || null != max_vol ||  null != min_vol || null != max_date || null !=min_date) {
			
			CompositeKeyWritable mapout = new CompositeKeyWritable(symbol,max_vol,max_date,min_vol,min_date);
			
			try {
				
				context.write(mapout,NullWritable.get());
			} catch (Exception e) {
				
				System.out.println("" + e.getMessage());

			}

		}

	}

}
