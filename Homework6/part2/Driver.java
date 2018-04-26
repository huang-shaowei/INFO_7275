package part2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

class Driver {
	
	public static void main(String[] args) throws Exception{
		
		if(args.length != 5) {
			System.err.println("Usage: MaxSubmittedCharge <input path> <output path>");
			System.exit(-1);
		}
		
		Path userinput = new Path(args[0]);
		Path ratinginput = new Path(args[1]);
		Path bookinput = new Path(args[2]);
		Path URoutput = new Path(args[3]);
		Path result = new Path(args[4]);
		
		
		Configuration conf = new Configuration();
		// Job 1
		Job job1 = Job.getInstance(conf);
		job1.setJarByClass(UserJoinMapper.class);
		
		MultipleInputs.addInputPath(job1, userinput, TextInputFormat.class, 
				UserJoinMapper.class);
		MultipleInputs.addInputPath(job1, ratinginput, TextInputFormat.class,
				RatingJoinMapper.class);
		FileOutputFormat.setOutputPath(job1, URoutput);
		job1.setReducerClass(UserRatingJoinReducer.class);
		job1.setNumReduceTasks(10);
		job1.setMapOutputKeyClass(Text.class);
		job1.setMapOutputValueClass(Text.class);
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(Text.class);

		
		FileSystem hdfs1 = FileSystem.get(conf);
		if (hdfs1.exists(URoutput)) {
			hdfs1.delete(URoutput, true);
		}
					
		if (!job1.waitForCompletion(true)) {
			System.exit(1);
		}
		
		// Job 2
		Job job2 = Job.getInstance(conf);
		job2.setJarByClass(URJoinMapper.class);
		
		MultipleInputs.addInputPath(job2, URoutput, TextInputFormat.class, 
				URJoinMapper.class);
		MultipleInputs.addInputPath(job2, bookinput, TextInputFormat.class,
				BookJoinMapper.class);
		FileOutputFormat.setOutputPath(job2, result);
		job2.setReducerClass(URBJoinReducer.class);
		job2.setNumReduceTasks(10);
		job2.setMapOutputKeyClass(Text.class);
		job2.setMapOutputValueClass(Text.class);
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(Text.class);

		
		FileSystem hdfs2 = FileSystem.get(conf);
		if (hdfs2.exists(result)) {
			hdfs2.delete(result, true);
		}
					
		if (!job2.waitForCompletion(true)) {
			System.exit(1);
		}
		
	}
}
