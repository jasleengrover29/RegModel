package com.Espire.FinalRegModel;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class RegDriver {

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();

		String[] programArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();

		if (programArgs.length != 2) {
			System.err.println("Usage: MaxTemp <in> <out>");
			System.exit(2);
		}

		Job job = new Job(conf, "Regression on Employee Data");
		job.setJarByClass(RegDriver.class);
		job.setMapperClass(RegMapper.class);

		job.setReducerClass(RegReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job, new Path(programArgs[0]));

		FileOutputFormat.setOutputPath(job, new Path(programArgs[0]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);

	}

}
