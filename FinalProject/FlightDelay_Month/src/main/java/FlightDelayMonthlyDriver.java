
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
//import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

class Driver {
	
	public static void main(String[] args) throws Exception{
		
		if(args.length != 2) {
			System.err.println("Usage: MaxSubmittedCharge <input path> <output path>");
			System.exit(-1);
		}
		
		Path flightdata = new Path(args[0]);
		Path totaldelay = new Path(args[1]);
		//Path bookinput = new Path(args[2]);
		//Path URoutput = new Path(args[3]);
		//Path result = new Path(args[4]);
		
		
		Configuration conf = new Configuration(true);
		// Job 1
		Job job1 = Job.getInstance(conf);
		job1.setJarByClass(FlightDelayMonthlyMapper.class);
		
		/*
		MultipleInputs.addInputPath(job1, userinput, TextInputFormat.class,UserJoinMapper.class);
		MultipleInputs.addInputPath(job1, ratinginput, TextInputFormat.class,RatingJoinMapper.class);
		*/

		
		job1.setMapperClass(FlightDelayMonthlyMapper.class);
		job1.setReducerClass(FlightDelayMonthlyReducer.class);
		job1.setNumReduceTasks(1);
		
		job1.setMapOutputKeyClass(IntWritable.class);
		job1.setMapOutputValueClass(DelayCountRatioTuple.class);
		job1.setOutputKeyClass(IntWritable.class);
		job1.setOutputValueClass(DelayCountRatioTuple.class);
		
		FileInputFormat.addInputPath(job1, flightdata);
		job1.setInputFormatClass(TextInputFormat.class);
		FileOutputFormat.setOutputPath(job1, totaldelay);
		
		FileSystem hdfs1 = FileSystem.get(conf);
		if (hdfs1.exists(totaldelay)) {
			hdfs1.delete(totaldelay, true);
		}
					
		if (!job1.waitForCompletion(true)) {
			System.exit(1);
		}
		
		/*
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
		*/
		
	}
}
