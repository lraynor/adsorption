package src; 

import org.apache.hadoop.mapreduce.*;
import java.io.IOException;

import org.apache.hadoop.io.*;

public class DiffMapper extends Mapper<LongWritable, Text, Text, Text> {

	public void map(LongWritable key, Text values, Context context) throws IOException, InterruptedException {
		String[] allVals = values.toString().split("\t");
		String node = allVals[0];
		String[] data = allVals[1].split(" ,");
		
		
		for (int i = 0; i < data.length; i++) {
			String curr = data[i];
			//ignore adjlist 
			if (!curr.contains("$ ")) {
				//otherwise map all labels to their node
				
				String[] parts = curr.split(" # ");
				/*
				for (int j = 0; j < 2; i++) {
					//parts[j] = parts[j].replace(" ", "");
					parts[j] = parts[j].replace("$ ", "");
				}
				*/
				String label = parts[0].replace(" ", "");
				Double weight = Double.parseDouble(parts[1]);
				//the key is the node and the value is the label
				//formatted as label # weight
				context.write(new Text(node), new Text(label + " # " + weight));
				
			}
		}

		//output the rank found using the vertex as the key since the 
		//vertex's ranks from two iterations must be compared
		//context.write(new Text(vertex), new Text(rank.toString() + ","));

	}

}