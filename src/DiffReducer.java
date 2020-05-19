package src; 

import org.apache.hadoop.mapreduce.*;
import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;

import org.apache.hadoop.io.*;

public class DiffReducer extends Reducer<Text, Text, Text, Text> {

	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		// expect to find two ranks denoted with # associated with this vertex, one for
		// each iteration
		String node = key.toString();
		int ctr = 0;
		HashMap<String, LinkedList<Double>> labels = new HashMap<String, LinkedList<Double>>();
		
		double[] ranks = new double[2];

		// parse out the two labels per label type
		for (Text value : values) {
			String label = value.toString();
			String parts[] = label.split(" # ");
			String type = parts[0].replace(" ", "");
			Double weight = Double.parseDouble(parts[1]);
			
			if (labels.get(type) == null) {
				labels.put(type, new LinkedList<Double>());
			}
			
			labels.get(type).add(weight);
		}
		
		
		//for each label type, emit the difference
		for (String type : labels.keySet()) {
			if (labels.get(type).size() < 2) {
				System.out.println("Something weird in DiffReducer: "); 
				System.out.println("Wrong number of labels, will skip this difference");
			} else {
				Double difference = Math.abs(labels.get(type).get(0) - labels.get(type).get(1));
				context.write(new Text("" + difference), new Text("N: " + node + " # L: " + type));
				
				
				
				
				
				
			}
			
			
			
		}
		

		//double difference = ranks[1] - ranks[0];
		// no longer need the vertex number, so just emit the rank difference
		//context.write(new Text("" + Math.abs(difference)), new Text(""));

	}

}