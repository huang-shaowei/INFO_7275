import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


class Driver {
	public static void main(String[] args) throws Exception {
		if (args.length != 2) {
			System.err.println("Usage: MaxSubmittedCharge <input path> <output path>");
			System.exit(-1);
		}

		Path inputPath = new Path(args[0]);
		Path outputDir = new Path(args[1]);

		// Create configuration
		Configuration conf = new Configuration(true);

		// Create job		
		Job job = Job.getInstance(conf);
		job.setJarByClass(DistinctRouteMapper.class);
		

		// Setup MapReduce
		job.setMapperClass(DistinctRouteMapper.class);
		job.setReducerClass(DistinctRouteReducer.class);
		job.setNumReduceTasks(1);

		// Specify key / value
	
		job.setMapOutputKeyClass(OriginDestTuple.class);
		job.setMapOutputValueClass(NullWritable.class);
		
		job.setOutputKeyClass(OriginDestTuple.class);
		job.setOutputValueClass(NullWritable.class);

		FileInputFormat.addInputPath(job, inputPath);
		FileOutputFormat.setOutputPath(job, outputDir);

		// Delete output if exists
		FileSystem hdfs = FileSystem.get(conf);
		if (hdfs.exists(outputDir))
			hdfs.delete(outputDir, true);

		// Execute job
		int code = job.waitForCompletion(true) ? 0 : 1;
		System.exit(code);
		
		

	}


}
