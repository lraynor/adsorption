package src; 

import org.apache.hadoop.mapreduce.*;
import java.io.IOException;
import org.apache.hadoop.io.*;

public class DiffReducer2 extends Reducer<FloatWritable, Text, Text, Text> {

	public void reduce(FloatWritable key, Text value, Context context) throws IOException, InterruptedException {

		// now the rank differences are sorted, so emit them as-is
		context.write(new Text(key.toString()), value);

	}

}