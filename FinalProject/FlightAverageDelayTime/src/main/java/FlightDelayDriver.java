import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


class FlightDelayDriver {
	public static void main(String[] args) throws Exception {
		if (args.length != 4) {
			System.err.println("Usage: MaxSubmittedCharge <input path> <output path>");
			System.exit(-1);
		}

		Path inputPath = new Path(args[0]);
		Path filterOutDir = new Path(args[1]);
		Path averageDelay = new Path(args[2]);
		Path delayRanking = new Path(args[3]);

		// Create configuration
		Configuration conf = new Configuration(true);

		// Job 1		
		
		Job job1 = Job.getInstance(conf, "filter out on-time");
		job1.setJarByClass(FlightDelayFilterMapper.class);
		job1.setMapperClass(FlightDelayFilterMapper.class);
		job1.setNumReduceTasks(0);
		
		job1.setMapOutputKeyClass(Text.class);
		job1.setMapOutputValueClass(NullWritable.class);
		
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(NullWritable.class);
		
		FileInputFormat.addInputPath(job1, inputPath);
		FileOutputFormat.setOutputPath(job1, filterOutDir);
		
		FileSystem hdfs1 = FileSystem.get(conf);
		if(hdfs1.exists(filterOutDir))
			hdfs1.delete(filterOutDir, true);
		
		if (!job1.waitForCompletion(true)) {
			System.exit(1);
		}
		
		// Job 2
		Job job2 = Job.getInstance(conf, "Calculate average delay time");
		job2.setJarByClass(FlightAverageDelayMapper.class);
		job2.setMapperClass(FlightAverageDelayMapper.class);
		job2.setReducerClass(FlightAverageDelayReducer.class);
		job2.setNumReduceTasks(1);
		
		job2.setMapOutputKeyClass(OriginDestTuple.class);
		job2.setMapOutputValueClass(DelayTimeAverageTuple.class);
		
		job2.setOutputKeyClass(OriginDestTuple.class);
		job2.setOutputValueClass(DelayTimeAverageTuple.class);
		
		FileInputFormat.addInputPath(job2, filterOutDir);
		FileOutputFormat.setOutputPath(job2, averageDelay);
		
		FileSystem hdfs2 = FileSystem.get(conf);
		if(hdfs2.exists(averageDelay))
			hdfs2.delete(averageDelay, true);
		
		if (!job2.waitForCompletion(true)) {
			System.exit(1);
		}
		
		// Job 3
				Job job3 = Job.getInstance(conf, "delay time ranking");
				job3.setJarByClass(DelayTimeRankingMapper.class);		
				job3.setMapperClass(DelayTimeRankingMapper.class);
				job3.setReducerClass(DelayTimeRankingReducer.class);
				job3.setNumReduceTasks(1);
				job3.setSortComparatorClass(DoubleComparator.class);
				job3.setMapOutputKeyClass(DoubleWritable.class);
				job3.setMapOutputValueClass(Text.class);
				job3.setOutputKeyClass(Text.class);
				job3.setOutputValueClass(DoubleWritable.class);
				
				FileInputFormat.addInputPath(job3, averageDelay);
				FileOutputFormat.setOutputPath(job3, delayRanking);
				
				FileSystem hdfs3 = FileSystem.get(conf);
				if (hdfs3.exists(delayRanking)) {
					hdfs3.delete(delayRanking, true);
				}
							
				if (!job3.waitForCompletion(true)) {
					System.exit(1);
				}
		
	}
}
