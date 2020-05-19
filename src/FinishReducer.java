package src; 

import org.apache.hadoop.mapreduce.*;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.apache.hadoop.io.*;

public class FinishReducer extends Reducer<Text, Text, Text, Text> {

	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

		List<Pair> allVertices = new ArrayList<Pair>();
		int ctr = 0; 
		for (Text value : values) {
			String curr = value.toString();

			ctr++; 
			System.out.println("kvpair received num " + ctr + " " + curr);
			String[] kvpair = curr.split(" & ");
			String node = kvpair[0];
			String label = kvpair[1];
	
			allVertices.add(new Pair(node, label));

		}

		//sort the list by ranks
		Collections.sort(allVertices, new Comparator<Pair>() {
			@Override
			public int compare(Pair first, Pair second) {
				return first.node.compareTo(second.node);
			}
		});

		
		
		String currNode = allVertices.get(0).node; 
		String currList = ""; 
		//output vertex, rank key value pairs by decreasing rank
		for (int i = 0; i < allVertices.size(); i++) {
			if (allVertices.get(i).node.contentEquals(currNode)) {
				currList += allVertices.get(i).label + ", ";
			} else {
				context.write(new Text(currNode), new Text(currList));
				currNode = allVertices.get(i).node; 
				currList = allVertices.get(i).label + ", "; 	
			}
			
		}
		
		
		Pair lastNode = allVertices.get(allVertices.size() - 1); 
		if (lastNode.node.equals(currNode)) {
			//currList += lastNode.label + ", ";
			context.write(new Text(currNode), new Text(currList));
		} else {
			context.write(new Text(lastNode.node), new Text(lastNode.label + ", "));
		}

	}

	class Pair {
		String node;
		String label;

		public Pair(String node, String label) {
			this.node = node;
			this.label = label;
		}
	}

}