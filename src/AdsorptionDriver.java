package src;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.lib.MultipleInputs;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class AdsorptionDriver {
	public static void main(String[] args) throws Exception {

		System.out.println("Name: Lynne Raynor, PennKey/SEAS login: lraynor");

		Configuration conf = new Configuration();

		if (args[0].contentEquals("init")) {
			Job job = Job.getInstance(conf, "init");

			job.setJarByClass(AdsorptionDriver.class);

			job.setMapperClass(InitMapper.class);
			job.setReducerClass(InitReducer.class);

			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(Text.class);

			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);

			setupInitIterFinish(job, args[1], args[2], args[3]);

			System.exit(job.waitForCompletion(true) ? 0 : 1);
		} else if (args[0].contentEquals("iter")) {
			Job job = Job.getInstance(conf, "iter");

			job.setJarByClass(AdsorptionDriver.class);

			job.setMapperClass(IterMapper.class);
			job.setReducerClass(IterReducer.class);

			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(Text.class);

			job.setOutputKeyClass(Text.class);

			
			FileInputFormat.addInputPaths(job, args[1]);
			deleteDirectory("intermediate_iter_results");
			FileOutputFormat.setOutputPath(job, new Path("intermediate_iter_results"));
			job.setNumReduceTasks(Integer.parseInt(args[3]));

			job.waitForCompletion(true);
			
			Job job2 = Job.getInstance(conf, "iter2");
			job2.setJarByClass(AdsorptionDriver.class);

			job2.setMapperClass(IterMapper2.class);
			job2.setReducerClass(IterReducer2.class);

			job2.setMapOutputKeyClass(Text.class);
			job2.setMapOutputValueClass(Text.class);

			job2.setOutputKeyClass(Text.class);
			job2.setOutputValueClass(Text.class);

			setupInitIterFinish(job2, "intermediate_iter_results", args[2], args[3]);
			

			System.exit(job2.waitForCompletion(true) ? 0 : 1);
			
			
		} else if (args[0].contentEquals("diff")) {
			Job job1 = Job.getInstance(conf, "diff1");

			job1.setJarByClass(AdsorptionDriver.class);

			job1.setMapperClass(DiffMapper.class);
			job1.setReducerClass(DiffReducer.class);

			job1.setMapOutputKeyClass(Text.class);
			job1.setMapOutputValueClass(Text.class);

			job1.setOutputKeyClass(Text.class);
			job1.setOutputValueClass(Text.class);

			FileInputFormat.addInputPaths(job1, args[1] + "," + args[2]);
			deleteDirectory("intermediate_diff_results");
			FileOutputFormat.setOutputPath(job1, new Path("intermediate_diff_results"));
			job1.setNumReduceTasks(Integer.parseInt(args[4]));

			job1.waitForCompletion(true);

			/////////////////////////////////////////////
			Job job2 = Job.getInstance(conf, "diff2");
			job2.setJarByClass(AdsorptionDriver.class);

			job2.setMapperClass(DiffMapper2.class);
			job2.setReducerClass(DiffReducer2.class);

			//job2.setMapOutputKeyClass(Text.class);
			job2.setMapOutputKeyClass(FloatWritable.class);
			job2.setMapOutputValueClass(Text.class);

			job2.setOutputKeyClass(Text.class);
			job2.setOutputValueClass(Text.class);

			setupInitIterFinish(job2, "intermediate_diff_results", args[3], args[4]);
			//////////////////////////////////////////////////////
			job2.setSortComparatorClass(SortFloatComparator.class);

			System.exit(job2.waitForCompletion(true) ? 0 : 1);
		} else if (args[0].contentEquals("finish")) {
			Job job = Job.getInstance(conf, "finish");

			job.setJarByClass(AdsorptionDriver.class);

			job.setMapperClass(FinishMapper.class);
			job.setReducerClass(FinishReducer.class);

			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(Text.class);

			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);

			setupInitIterFinish(job, args[1], args[2], args[3]);

			System.exit(job.waitForCompletion(true) ? 0 : 1);
		} else if (args[0].contentEquals("composite")) {
			//parse out all necessary command line args
			String inputDir = args[1];
			String outputDir = args[2];
			String intermDir1 = args[3];
			String intermDir2 = args[4];
			String diffDir = args[5];
			String numReducers = args[6];

			//the init job
			Job job = Job.getInstance(conf, "init");

			job.setJarByClass(AdsorptionDriver.class);
			job.setMapperClass(InitMapper.class);
			job.setReducerClass(InitReducer.class);

			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(Text.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);

			setupInitIterFinish(job, inputDir, intermDir1, numReducers);
			
			job.waitForCompletion(true);

			int ctr = 0;
			double diff = Double.MAX_VALUE;
			while (diff > .2) {
	
				//Run the first iteration in 2 jobs
				Job itrjob1a = Job.getInstance(conf, "iter");

				itrjob1a.setJarByClass(AdsorptionDriver.class);

				itrjob1a.setMapperClass(IterMapper.class);
				itrjob1a.setReducerClass(IterReducer.class);

				itrjob1a.setMapOutputKeyClass(Text.class);
				itrjob1a.setMapOutputValueClass(Text.class);

				itrjob1a.setOutputKeyClass(Text.class);

				
				FileInputFormat.addInputPaths(itrjob1a, intermDir1);
				deleteDirectory("iterTemp1");
				FileOutputFormat.setOutputPath(itrjob1a, new Path("iterTemp1"));
				itrjob1a.setNumReduceTasks(Integer.parseInt(numReducers));

				itrjob1a.waitForCompletion(true);
				
				Job iterjob1b = Job.getInstance(conf, "iter2");
				iterjob1b.setJarByClass(AdsorptionDriver.class);

				iterjob1b.setMapperClass(IterMapper2.class);
				iterjob1b.setReducerClass(IterReducer2.class);

				iterjob1b.setMapOutputKeyClass(Text.class);
				iterjob1b.setMapOutputValueClass(Text.class);

				iterjob1b.setOutputKeyClass(Text.class);
				iterjob1b.setOutputValueClass(Text.class);

				setupInitIterFinish(iterjob1b, "iterTemp1", intermDir2, numReducers);
				iterjob1b.waitForCompletion(true);
						
				//Run the second iteration
				Job itrjob2a = Job.getInstance(conf, "iter");

				itrjob2a.setJarByClass(AdsorptionDriver.class);

				itrjob2a.setMapperClass(IterMapper.class);
				itrjob2a.setReducerClass(IterReducer.class);

				itrjob2a.setMapOutputKeyClass(Text.class);
				itrjob2a.setMapOutputValueClass(Text.class);

				itrjob2a.setOutputKeyClass(Text.class);

				
				FileInputFormat.addInputPaths(itrjob2a, intermDir2);
				deleteDirectory("iterTemp2");
				FileOutputFormat.setOutputPath(itrjob2a, new Path("iterTemp2"));
				itrjob2a.setNumReduceTasks(Integer.parseInt(numReducers));

				itrjob2a.waitForCompletion(true);
				
				Job iterjob2b = Job.getInstance(conf, "iter2");
				iterjob2b.setJarByClass(AdsorptionDriver.class);

				iterjob2b.setMapperClass(IterMapper2.class);
				iterjob2b.setReducerClass(IterReducer2.class);

				iterjob2b.setMapOutputKeyClass(Text.class);
				iterjob2b.setMapOutputValueClass(Text.class);

				iterjob2b.setOutputKeyClass(Text.class);
				iterjob2b.setOutputValueClass(Text.class);

				setupInitIterFinish(iterjob2b, "iterTemp2", intermDir1, numReducers);
				iterjob2b.waitForCompletion(true);
				
				
				
				
				//next two jobs handle the diff operation
				//using the two iterations above
				Job job1 = Job.getInstance(conf, "diff1");

				job1.setJarByClass(AdsorptionDriver.class);
				job1.setMapperClass(DiffMapper.class);
				job1.setReducerClass(DiffReducer.class);

				job1.setMapOutputKeyClass(Text.class);
				job1.setMapOutputValueClass(Text.class);
				job1.setOutputKeyClass(Text.class);
				job1.setOutputValueClass(Text.class);

				FileInputFormat.addInputPaths(job1, intermDir1 + "," + intermDir2);
				job1.setNumReduceTasks(Integer.parseInt(numReducers));
				deleteDirectory("intermediate_diff_results");
				FileOutputFormat.setOutputPath(job1, new Path("intermediate_diff_results"));

				job1.waitForCompletion(true);

				Job job2 = Job.getInstance(conf, "diff2");
				job2.setJarByClass(AdsorptionDriver.class);

				job2.setMapperClass(DiffMapper2.class);
				job2.setReducerClass(DiffReducer2.class);

				job2.setMapOutputKeyClass(FloatWritable.class);
				job2.setMapOutputValueClass(Text.class);

				job2.setOutputKeyClass(Text.class);
				job2.setOutputValueClass(Text.class);

				setupInitIterFinish(job2, "intermediate_diff_results", diffDir, numReducers);
				job2.setSortComparatorClass(SortFloatComparator.class);
				job2.waitForCompletion(true);

				diff = readDiffResult(diffDir);
				System.out.println("iter " + ctr + ": diff was read from file : " + diff);
				ctr++; 

			}

			Job finishjob = Job.getInstance(conf, "finish");

			finishjob.setJarByClass(AdsorptionDriver.class);

			finishjob.setMapperClass(FinishMapper.class);
			finishjob.setReducerClass(FinishReducer.class);

			finishjob.setMapOutputKeyClass(Text.class);
			finishjob.setMapOutputValueClass(Text.class);

			finishjob.setOutputKeyClass(Text.class);
			finishjob.setOutputValueClass(Text.class);

			setupInitIterFinish(finishjob, intermDir2, outputDir, "1");
			System.exit(finishjob.waitForCompletion(true) ? 0 : 1);

		}

	}

	// configures input/output directories and number of reducer nodes for init,
	// iter, and finish jobs
	static void setupInitIterFinish(Job job, String input, String output, String numReducers) throws Exception {
		FileInputFormat.addInputPath(job, new Path(input));
		deleteDirectory(output);
		FileOutputFormat.setOutputPath(job, new Path(output));
		job.setNumReduceTasks(Integer.parseInt(numReducers));
	}

	// modified to read multiple files for MS2
	static double readDiffResult(String path) throws Exception {
		Double diffnum = 1.6;
		Path diffpath = new Path(path);
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(URI.create(path), conf);

		List<Double> maxPerFile = new ArrayList<Double>();
		if (fs.exists(diffpath)) {
			FileStatus[] ls = fs.listStatus(diffpath);
			for (FileStatus file : ls) {
				if (file.getPath().getName().startsWith("part-r-")) {
					FSDataInputStream diffin = fs.open(file.getPath());
					BufferedReader d = new BufferedReader(new InputStreamReader(diffin));
					String diffcontent = d.readLine();
					System.out.println("line read in diff: " + diffcontent);
					if (diffcontent != null) {
						String[] kv = diffcontent.split("\t");
						diffnum = Double.parseDouble(kv[0]);
						if (!(diffnum == null)) {
							maxPerFile.add(diffnum);
						}
					}
					d.close();
				}
			}
		}

		fs.close();
		Collections.sort(maxPerFile);

		return maxPerFile.get(maxPerFile.size() - 1);
	}

	static void deleteDirectory(String path) throws Exception {
		Path todelete = new Path(path);
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(URI.create(path), conf);

		if (fs.exists(todelete))
			fs.delete(todelete, true);

		fs.close();
	}

	// used to sort diff kv pairs from high to low instead of low to high
	// going from diffmapper2 to diffreducer2
	public static class SortFloatComparator extends WritableComparator {

		protected SortFloatComparator() {
			super(FloatWritable.class, true);
		}

		@SuppressWarnings("rawtypes")

		@Override
		public int compare(WritableComparable w1, WritableComparable w2) {
			FloatWritable k1 = (FloatWritable) w1;
			FloatWritable k2 = (FloatWritable) w2;

			return -1 * k1.compareTo(k2);
		}
	}

}
