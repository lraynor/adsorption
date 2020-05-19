package src; 

import org.apache.hadoop.mapreduce.*;
import java.io.IOException;

import org.apache.hadoop.io.*;

public class DiffMapper2 extends Mapper<LongWritable, Text, FloatWritable, Text> {

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		String[] kvpair = value.toString().split("\t");
		Float diff = Float.parseFloat(kvpair[0]);
		//Float diff = Float.parseFloat(value.toString());

		//emit using the rank differences as the keys so that the shuffle phase
		//will sort them using the custom Comparator defined in the Driver
		context.write(new FloatWritable(diff), new Text(kvpair[1]));

	}

}