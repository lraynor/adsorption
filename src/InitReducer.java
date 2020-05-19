package src; 
import org.apache.hadoop.mapreduce.*;
import java.io.IOException;
import org.apache.hadoop.io.*;


public class InitReducer extends Reducer<Text, Text, Text, Text> {

	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		
		String adjList = "";
		
		for (Text value : values) {
			adjList += value.toString(); 
		}

		//finalize the output for iteration, each key will be a node, each value will contain
		//an adjacency list of outnodes formatted as $ outnode # weight
		//and a list of labels formatted as label weight
		//self-labels should be hardcoded to 1
		//System.out.println("got to reduce"); 
		//System.out.println("new key " + key.toString());
		//System.out.println("new value + " + key.toString() + " # 1," + values.toString());
		context.write(key, new Text(key.toString() + " # 1 ," + adjList));

	}

}
