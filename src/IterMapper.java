package src; 
import org.apache.hadoop.mapreduce.*;
import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.io.*;

public class IterMapper extends Mapper<LongWritable, Text, Text, Text> {

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		//parse out labels from neighbors
		
		String[] kvpair = value.toString().split("\t");
		String node = kvpair[0];
		String info[] = kvpair[1].split(" ,");
		
		HashMap<String, Double> adjList = new HashMap<String, Double>();
		HashMap<String, Double> labels = new HashMap<String, Double>();
		
		
		for (String item : info) {
			if (item.startsWith("$ ") ) {
				
				
				//adj list denoted with $
				String[] parts = item.split(" # ");
				System.out.println("outnode before " + parts[0]);
				parts[0] = parts[0].replace("$ ", "");
				parts[0] = parts[0].replace(" ", "");
				parts[1] = parts[1].replace(" ", "");
				
				System.out.println("outnode after " + parts[0]);
				String outnode = parts[0]; 
				Double weight = Double.parseDouble(parts[1]);
				adjList.put(outnode, weight);
				
				if (weight > .001) {
					//the graph edges don't change, so propagate as-is
					context.write(new Text(node), new Text(item));
				}
				
				
				
			} else {
				//other entries are labels
				String[] parts = item.split(" # ");
				parts[0] = parts[0].replace(" ", "");
				parts[1] = parts[1].replace(" ", "");
				
				String label = parts[0]; 
				Double weight = Double.parseDouble(parts[1]);
				labels.put(label, weight);
			}
		}
	
//		System.out.println("size of labels ? " + labels.size());
		
		//double for loop to propagate labels
		//each outward edge gets a copy of every label
		for (String outnode : adjList.keySet()) {
			Double edgeWeight = adjList.get(outnode);
			
			
			for (String label : labels.keySet()) {
				Double labelWeight = labels.get(label);
				Double newWeight = edgeWeight * labelWeight; 
				
				
				///////////possible logical error
				context.write(new Text(label), new Text(outnode + " # " + newWeight));
				
			}
	
		}
		


	}

}