package src; 

import org.apache.hadoop.mapreduce.*;
import java.io.IOException;

import org.apache.hadoop.io.*;

public class FinishMapper extends Mapper<LongWritable, Text, Text, Text> {

	public void map(LongWritable key, Text values, Context context) throws IOException, InterruptedException {
		String[] kvpair = values.toString().split("\t");
		String node = kvpair[0];
		String[] allVals = kvpair[1].split(",");
		
		
		
		for (String entry : allVals) {
			if (!entry.contains("$ ")) {
				context.write(new Text("same for all"), new Text(node + " & " + entry));
			}
			
			
			
			
		}
		

		/*
		for (int i = 0; i < outNeighbors.length; i++) {
			if (outNeighbors[i].contains("#")) {
				// find the rank of the vertex, denoted by #
				rank = Double.parseDouble(outNeighbors[i].replaceAll("[^\\.\\d]", ""));
			}
		}
	*/
		// write all ranks to the same key for sorting by value
		//context.write(new Text("same for all"), new Text(vertex + "," + rank + ""));

	}

}