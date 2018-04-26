import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class PutMerge {
	
	public static void main (String[] args) throws IOException {
		
		Configuration conf = new Configuration();
		FileSystem hdfs = FileSystem.get(conf);
		FileSystem local = FileSystem.getLocal(conf);
		
		Path inputDir = new Path(args[0]);
		Path outputFile = new Path(args[1]);
		
		try {
			FileStatus[] inputFiles = hdfs.listStatus(inputDir);
			FSDataOutputStream out = hdfs.create(outputFile);
			
			for (int i=0; i < inputFiles.length; i++) {
				
				System.out.println(inputFiles[i].getPath().getName());
				FSDataInputStream in = hdfs.open(inputFiles[i].getPath());
				
				byte buffer[] = new byte[256];
				int bytesRead = 0;
				while ( (bytesRead = in.read(buffer)) > 0) {
					out.write(buffer, 0, bytesRead);
				}
				in.close();
			}
			out.close();		
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
}
