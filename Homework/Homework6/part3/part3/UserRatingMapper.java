package part3;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;

public class UserRatingMapper extends Mapper<Object, Text, Text, Text>{
	
	private URI[] files;
	private HashMap<String, String> MemberMap = new HashMap<String,String>();
	private Text outkey = new Text();
	private Text outvalue = new Text();
	
	public void setup(Context context) throws IOException {
		files = DistributedCache.getCacheFiles(context.getConfiguration());
		Path path = new Path(files[0]);
		
		FileSystem fs = FileSystem.get(context.getConfiguration());
		FSDataInputStream in = fs.open(path);
		BufferedReader br = new BufferedReader(new InputStreamReader(in));
		
		String line = "";
		while ((line = br.readLine()) != null) {
			String[] splits = line.split(";");
			MemberMap.put(splits[0], splits[1] + " " + splits[2]);
		}
		
		br.close();
		in.close();
	}
	
	public void map(Object key, Text value, Context context) 
			throws IOException, InterruptedException {
		
		String[] fields = value.toString().split(";");
		if(MemberMap.containsKey(fields[0])) {
			outkey.set(value);
			outvalue.set(MemberMap.get(fields[0]));
		}
		context.write(outkey,outvalue);
	}
} 
