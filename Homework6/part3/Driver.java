package part3;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

class Driver extends Configured implements Tool{
	
	public static void main(String[] args) throws Exception{
		ToolRunner.run(new Driver(), args);
	}
	
	public int run(String[] args) throws Exception {
		Path inputPath = new Path(args[0]);
		Path outputDir = new Path(args[1]);
		Configuration conf = new Configuration();
		
		DistributedCache.addCacheFile(new URI("/home/wei/eclipse-workspace/MRReduceSideJoin/input/BX-Book-Ratings.csv"), conf);
		
		Job job = new Job(conf, "Distributed Cache");
		job.setJarByClass(UserRatingMapper.class);
		job.setMapperClass(UserRatingMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setNumReduceTasks(0);
		
		FileInputFormat.addInputPath(job, inputPath);
		FileOutputFormat.setOutputPath(job, outputDir);
		FileSystem hdfs = FileSystem.get(conf);
		if (hdfs.exists(outputDir))
			hdfs.delete(outputDir, true);
		
		job.waitForCompletion(true);
		
		return 0;
		
	}
	
	
}
