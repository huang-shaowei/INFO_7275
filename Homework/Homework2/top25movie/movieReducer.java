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
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;




public class movieReducer extends Reducer<Text, DoubleWritable, DoubleWritable, Text>{
	
	
	private DoubleWritable result = new DoubleWritable();
	
	private Text name = new Text();
	
	public void reduce(Text key,Iterable<DoubleWritable> values,Context context)
            throws IOException, InterruptedException {
	  
	double sum =0;
	double count=0;
	
	  for(DoubleWritable val :values){
		  try {		  
			  sum += val.get();
			  count++;
			  
		} catch (Exception e) {
			// TODO: handle exception
		}
	  }
	  
	  result.set(sum/count);
	  
	  name.set(key);
	  
	  
	  context.write(result,name);
	  
	  
	  
	  
	 
	  //countMap.put(result, key);
	 
	  
	  //context.write(key, result);
	  
	  //System.out.println(countMap);
	  //context.write(result, name);
	  
	  /*
	  Set<DoubleWritable> keySet = countMap.keySet();
	  Iterator<DoubleWritable> iter = keySet.iterator();
	  while(iter.hasNext()) {
		  DoubleWritable key_d = iter.next();
		  System.out.println(key_d + " " + countMap.get(key_d));
		  context.write(key_d,countMap.get(key_d));
	  }*/
	  
	  
	  
	  //Map<DoubleWritable, Text> resultMap = sortMapByKey(countMap);
	  /*Map<DoubleWritable, Text> resultMap = countMap.descendingMap();
	  int t =0;
	  
	 
	  for(Map.Entry<DoubleWritable, Text> entry : resultMap.entrySet()) {
		  
		  System.out.println(entry.getKey() + " " + entry.getValue());
		  context.write(entry.getKey(),entry.getValue());
		  
		  
		  
	  }*/
	  
	  
	  
	  //context.write(name, countMap.get(name));
	  
	  //context.write(name, result);
       
    }

	
	

}
