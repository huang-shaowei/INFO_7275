package top25movie;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import java.util.Iterator;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

import com.sun.tools.javac.util.List;
public class ratingReducer 	extends Reducer <DoubleWritable, Text,Text, DoubleWritable>{
	
	private Text result = new Text();
	private DoubleWritable rate  = new DoubleWritable();
	private int nCount = 0;
	protected void setup(Context context) throws IOException, InterruptedException {
		nCount=0;
	}
	@Override
	public void reduce(DoubleWritable key, Iterable<Text> values, Context context) {
		if(nCount <24) {
			try {
				
				for (Text val :values) {
					result.set(val.toString());
					
					context.write(val, key);
					nCount++;
					if(nCount>24) {
						break;
					}
				}
				
			} catch (Exception e) {
				
			}
		}
	}
}
