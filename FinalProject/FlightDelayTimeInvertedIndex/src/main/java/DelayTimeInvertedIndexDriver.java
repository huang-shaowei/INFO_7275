import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

class DelayTimeInvertedIndexDriver {
	public static void main(String[] args) throws Exception {
		
		if(args.length != 2) {
			System.err.println("Usage: MaxSubmittedCharge <input path> <output path>");
			System.exit(-1);
		}
		
		Path inputPath = new Path(args[0]);
		Path outputDir = new Path(args[1]);
			
		Configuration conf = new Configuration();
		
		// Job 1
		Job job1 = Job.getInstance(conf);
		job1.setJarByClass(DelayTimeInvertedIndexMapper.class);
		
		job1.setMapperClass(DelayTimeInvertedIndexMapper.class);
		job1.setReducerClass(DelayTimeInvertedIndexReducer.class);
		
		job1.setNumReduceTasks(1);
		
		job1.setMapOutputKeyClass(Text.class);
		job1.setMapOutputValueClass(Text.class);
		
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(job1, inputPath);
		FileOutputFormat.setOutputPath(job1, outputDir);
		
		FileSystem hdfs1 = FileSystem.get(conf);
		if (hdfs1.exists(outputDir)) {
			hdfs1.delete(outputDir, true);
		}
			
		if (!job1.waitForCompletion(true)) {
			System.exit(1);
		}
		
		
	}
}
