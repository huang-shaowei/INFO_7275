package part4;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

class TagMapper extends Mapper<Object, Text, Text, Text>{
	
	private Text outkey = new Text();
	private Text outvalue = new Text();
	
	public void map(Object key, Text value, Context context) 
			throws IOException, InterruptedException {
		
		String[] fields = value.toString().split(",");
		try {
			//if(!fields[0].equals("userId")) {
				outkey.set(fields[1]);
				outvalue.set("T" + fields[2].toString());
				//System.out.println(outkey + "\t" + outvalue);
			//}
		} catch (Exception e) {
			
		}
		context.write(outkey, outvalue);
	}
}

