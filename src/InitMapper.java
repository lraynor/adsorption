package src;

import org.apache.hadoop.mapreduce.*;
import java.io.IOException;

import org.apache.hadoop.io.*;

public class InitMapper extends Mapper<LongWritable, Text, Text, Text> {


	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String[] edge = value.toString().split("\t");
		String node = edge[0];
		String outnode = edge[1];
		Double weight = Double.parseDouble(edge[2]);

		//propagate the edge as the kv pair (node, $outnode # weight,)
		context.write(new Text(node), new Text("$ " + outnode + " # " + weight + " ,"));
		context.write(new Text(outnode), new Text(""));
		
		
		/*
		//add a self-label as the kv pair (node, $label#weight,)
		//weight of self-labels should always be hardcoded to 1
		context.write(new Text(node), new Text("$" + node + "#1,"));
		
		// initialize a self-label for the outnode, in case it is
		// a sink in the input graph
		context.write(new Text(outnode), new Text("$" + outnode + "#1,"));
		*/
	}

}