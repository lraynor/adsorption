package src; 

import org.apache.hadoop.mapreduce.*;
import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;

import org.apache.hadoop.io.*;

public class IterReducer extends Reducer<Text, Text, Text, Text> {

	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

		String node = key.toString();
		String newNodeValues = "";

		
		HashMap<String, Double> adjList = new HashMap<String, Double>();
		HashMap<String, LinkedList<Double>> labels = new HashMap<String, LinkedList<Double>>();
		
		
		for (Text value : values) {
			String item = value.toString();
			if (item.startsWith("$ ") ) {
				//the graph edges don't change, so propagate as-is
				context.write(new Text(node), new Text(item));
				newNodeValues += item + " ,"; 
				
				//adj list denoted with $
				String[] parts = item.split(" # ");
	
				parts[0] = parts[0].replace("$ ", "");
				parts[0] = parts[0].replace(" ", "");
				parts[1] = parts[1].replace(" ", "");
				
				String outnode = parts[0]; 
				Double weight = Double.parseDouble(parts[1]);
				adjList.put(outnode, weight);
				
			} else {
				//other entries are labels
				String[] parts = item.split(" # ");
				parts[0] = parts[0].replace(" ", "");
				parts[1] = parts[1].replace(" ", "");
				
							String nodeOwner = parts[0]; 
				Double weight = Double.parseDouble(parts[1]);
				if (labels.get(nodeOwner) == null) {
					labels.put(nodeOwner, new LinkedList<Double>());
				}
				
				labels.get(nodeOwner).add(weight);
			}
		}
		
		
		String label = key.toString();
		
		//sum labels for normalization
		double sum = 0; 
		for (String owner : labels.keySet()) {
			//don't do anything for self-label, which is hardcoded later
			if (!owner.equals(label)) {
				//otherwise calculate new label	
				for (Double weight : labels.get(owner)) {
					sum += weight; 
				}
			}
			
		}
		
		
		for (String owner : labels.keySet()) {
			if (!owner.equals(label)) {
				System.out.println("reading through the labels now");
				for (Double weight : labels.get(owner)) {
					if (weight > .01) {
						context.write(new Text(owner), new Text(label + " # " + weight / sum + ""));
					} else {
						System.out.println("filtered out low value: key = " + owner + " value = " + label + " # " + weight/sum);
					}
				}
			}
		}

		
		//hardcode self-label to 1
		context.write(new Text(node), new Text(node + " # 1"));

	}

}