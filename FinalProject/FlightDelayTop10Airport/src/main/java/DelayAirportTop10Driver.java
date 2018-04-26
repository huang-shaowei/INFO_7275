import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
//import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


class Driver {
	
	public static void main(String[] args) throws Exception{
		
		if(args.length != 3) {
			System.err.println("Usage: MaxSubmittedCharge <input path> <output path>");
			System.exit(-1);
		}
		
		Path flightdata = new Path(args[0]);
		Path delayairportOut = new Path(args[1]);
		Path top10Result = new Path(args[2]);

		Configuration conf = new Configuration(true);
		// Job 1
		Job job1 = Job.getInstance(conf, "airport delay ratio");
		job1.setJarByClass(DelayAirportMapper.class);
		
		/*
		MultipleInputs.addInputPath(job1, userinput, TextInputFormat.class,UserJoinMapper.class);
		MultipleInputs.addInputPath(job1, ratinginput, TextInputFormat.class,RatingJoinMapper.class);
		*/

		
		job1.setMapperClass(DelayAirportMapper.class);
		job1.setReducerClass(DelayAirportReducer.class);
		job1.setNumReduceTasks(1);
		
		job1.setMapOutputKeyClass(Text.class);
		job1.setMapOutputValueClass(DelayCountRatioTuple.class);
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(DelayCountRatioTuple.class);
		
		FileInputFormat.addInputPath(job1, flightdata);
		job1.setInputFormatClass(TextInputFormat.class);
		FileOutputFormat.setOutputPath(job1, delayairportOut);
		
		FileSystem hdfs1 = FileSystem.get(conf);
		if (hdfs1.exists(delayairportOut)) {
			hdfs1.delete(delayairportOut, true);
		}
					
		if (!job1.waitForCompletion(true)) {
			System.exit(1);
		}
		
		
		// Job 2
		Job job2 = Job.getInstance(conf, "delay ratio ranking");
		job2.setJarByClass(DelayRankingMapper.class);		
		job2.setMapperClass(DelayRankingMapper.class);
		job2.setReducerClass(DelayRankingReducer.class);
		job2.setNumReduceTasks(1);
		job2.setSortComparatorClass(DoubleComparator.class);
		job2.setMapOutputKeyClass(DoubleWritable.class);
		job2.setMapOutputValueClass(Text.class);
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(DoubleWritable.class);
		
		FileInputFormat.addInputPath(job2, delayairportOut);
		FileOutputFormat.setOutputPath(job2, top10Result);
		
		FileSystem hdfs2 = FileSystem.get(conf);
		if (hdfs2.exists(top10Result)) {
			hdfs2.delete(top10Result, true);
		}
					
		if (!job2.waitForCompletion(true)) {
			System.exit(1);
		}
		
		
	}
}

