package src; 

import org.apache.hadoop.mapreduce.*;
import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;

import org.apache.hadoop.io.*;

public class IterReducer2 extends Reducer<Text, Text, Text, Text> {

	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
//		System.out.println("got to iter reduce");
//		System.out.println("key = " + key.toString());
//		System.out.println("value = " + values.toString());
		

		String node = key.toString();
		String newNodeValues = "";
/*
		//parse out labels from neighbors
		String allVals = ""; 
		
		System.out.println();
		int ctr = 0; 
		for (Text value : values) {
			System.out.println("key: " + node + " value num " + ctr  + ": " + value.toString());
			allVals += value.toString(); 
			
		}
		
	*/
		
		HashMap<String, Double> adjList = new HashMap<String, Double>();
		HashMap<String, LinkedList<Double>> labels = new HashMap<String, LinkedList<Double>>();
		
		
		for (Text value : values) {
			System.out.println("IR2 key = " + node);
			System.out.println("IR value = " + value.toString());
			String item = value.toString();
			if (item.startsWith("$ ") ) {
				//the graph edges don't change, so propagate as-is
				//context.write(new Text(node), new Text(item + " ,"));
				newNodeValues += item + " ,"; 
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
				
				
			} else {
				//other entries are labels
				String[] parts = item.split(" # ");
				parts[0] = parts[0].replace(" ", "");
				parts[1] = parts[1].replace(" ", "");
				
				String label = parts[0]; 
				Double weight = Double.parseDouble(parts[1]);
				if (labels.get(labels) == null) {
					labels.put(label, new LinkedList<Double>());
					//LinkedList<Double> toAdd = new LinkedList<Double>(); 
				}
				
				labels.get(label).add(weight);
			}
		}
		
		
		
		//aggregate list of labels associated with this node for intermediate kvpair
		for (String label : labels.keySet()) {
			//don't do anything for self-label, which is hardcoded later
			if (!label.equals(node)) {
				
				//otherwise calculate new label
				double sum = 0; 
				for (Double weight : labels.get(label)) {
					sum += weight; 
				}
				
				newNodeValues += label + " # " + sum + " ,";
				
			}
			
			
		}

		
		//hardcode self-label to 1
		context.write(new Text(node), new Text(node + " # 1 ," + newNodeValues));

	}

}