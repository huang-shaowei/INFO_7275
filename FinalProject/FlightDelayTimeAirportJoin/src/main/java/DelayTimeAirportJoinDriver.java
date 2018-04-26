import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

class DelayTimeAirportJoinDriver {
	
	public static void main(String[] args) throws Exception{
		
		if(args.length != 3) {
			System.err.println("Usage: MaxSubmittedCharge <input path> <output path>");
			System.exit(-1);
		}
		
		Path flightInput = new Path(args[0]);
		Path airportInput = new Path(args[1]);
		Path fightAirportJoin = new Path(args[2]);
		
		
		Configuration conf = new Configuration();
		// Job 1
		Job job1 = Job.getInstance(conf);
		job1.setJarByClass(DelayTimeJoinMapper.class);
		
		MultipleInputs.addInputPath(job1, flightInput, TextInputFormat.class, 
				DelayTimeJoinMapper.class);
		MultipleInputs.addInputPath(job1, airportInput, TextInputFormat.class,
				AirportJoinMapper.class);
		FileOutputFormat.setOutputPath(job1, fightAirportJoin);
		job1.setReducerClass(DelayTimeAirportJoinReducer.class);
		job1.setNumReduceTasks(10);
		job1.setMapOutputKeyClass(Text.class);
		job1.setMapOutputValueClass(Text.class);
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(Text.class);

		
		FileSystem hdfs1 = FileSystem.get(conf);
		if (hdfs1.exists(fightAirportJoin)) {
			hdfs1.delete(fightAirportJoin, true);
		}
					
		if (!job1.waitForCompletion(true)) {
			System.exit(1);
		}
		
	}
}
