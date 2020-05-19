package src; 
import org.apache.hadoop.mapreduce.*;
import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.io.*;

public class IterMapper2 extends Mapper<LongWritable, Text, Text, Text> {

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		//parse out labels from neighbors
		
		String[] kvpair = value.toString().split("\t");
		String node = kvpair[0];
		
		context.write(new Text(node), new Text(kvpair[1]));
		


	}

}