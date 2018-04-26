package part4;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;



class Driver {
	public static void main(String[] args) throws Exception{
		
		if(args.length != 3) {
			System.err.println("Usage: MaxSubmittedCharge <input path> <output path>");
			System.exit(-1);
		}
		
		Path movieIn = new Path(args[0]);
		Path tagIn = new Path(args[1]);
		Path output = new Path(args[2]);
		
		Configuration conf = new Configuration();
		// Job 
		Job job = Job.getInstance(conf);
		job.setJarByClass(MovieMapper.class);
		
		MultipleInputs.addInputPath(job, movieIn, TextInputFormat.class, 
				MovieMapper.class);
		MultipleInputs.addInputPath(job, tagIn, TextInputFormat.class,
				TagMapper.class);
		FileOutputFormat.setOutputPath(job, output);
		
		job.setReducerClass(MovieTagHierarchyReducer.class);
		job.setNumReduceTasks(1);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);

		
		FileSystem hdfs = FileSystem.get(conf);
		if (hdfs.exists(output)) {
			hdfs.delete(output, true);
		}
					
		if (!job.waitForCompletion(true)) {
			System.exit(1);
		}
	}
}
