package com.Espire.FinalRegModel;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class RegMapper extends Mapper<LongWritable, Text, Text, Text> {

	@Override
	public void map(LongWritable key, Text value, Context context)

	throws IOException, InterruptedException {

		String line = value.toString();
		StringTokenizer st = new StringTokenizer(line, ",");

		int y = Integer.parseInt(st.nextToken());
		int x1 = Integer.parseInt(st.nextToken());
		int x2 = Integer.parseInt(st.nextToken());

		String input = y + "#" + x1 + "#" + x2;

		context.write(new Text("Input value: "), new Text(input));

	}
}
