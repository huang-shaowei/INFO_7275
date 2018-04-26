package top25movie;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;



import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;

public class Driver {

	public static void main(String[] args) throws Exception {
		if (args.length != 3) {
			System.err.println("Usage: MaxSubmittedCharge <input path> <output path>");
			System.exit(-1);
		}

		Path inputPath = new Path(args[0]);
		Path outputDir = new Path(args[1]);
		Path resultPath = new Path(args[2]);

		// Create configuration
		Configuration conf = new Configuration(true);

		// Create job		
		Job job1 = Job.getInstance(conf,"movie rating");
		job1.setJarByClass(movieMapper.class);
		

		// Setup MapReduce
		job1.setMapperClass(movieMapper.class);
		job1.setReducerClass(movieReducer.class);
		job1.setNumReduceTasks(1);
		job1.setMapOutputKeyClass(Text.class);
		job1.setMapOutputValueClass(DoubleWritable.class);	
		job1.setOutputKeyClass(DoubleWritable.class);
		job1.setOutputValueClass(Text.class);
		// Input
		FileInputFormat.addInputPath(job1, inputPath);
		job1.setInputFormatClass(TextInputFormat.class);
		FileOutputFormat.setOutputPath(job1, new Path(outputDir, "out1"));
		/*
		FileSystem hdfs = FileSystem.get(conf);
		if (hdfs.exists(outputDir))
			hdfs.delete(outputDir, true);*/
		// Output
		
		if (!job1.waitForCompletion(true)) {
			  System.exit(1);
			}
	
		
		Job job2 = Job.getInstance(conf, "sort by rating");
		job2.setJarByClass(ratingMapper.class);
		job2.setMapperClass(ratingMapper.class);
		job2.setReducerClass(ratingReducer.class);
		job2.setNumReduceTasks(1);
		job2.setSortComparatorClass(DoubleComparator.class);
		job2.setMapOutputKeyClass(DoubleWritable.class);
		job2.setMapOutputValueClass(Text.class);	
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(DoubleWritable.class);
		//job2.setInputFormatClass(TextFileInputFormat.class);
		FileInputFormat.addInputPath(job2, new Path(outputDir, "out1"));
		FileOutputFormat.setOutputPath(job2, new Path(resultPath, "out2"));
		
		/*
		FileSystem hdfs2 = FileSystem.get(conf);
		if (hdfs2.exists(outputDir))
			hdfs2.delete(outputDir, true);
	*/
		if (!job2.waitForCompletion(true)) {
			  System.exit(1);
			}
		/* Delete output if exists
		
		Execute job
		int code = job1.waitForCompletion(true) ? 0 : 1;
		System.exit(code);
		 */
	}

}
