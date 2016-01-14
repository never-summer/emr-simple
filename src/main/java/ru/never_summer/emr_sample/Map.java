package ru.never_summer.emr_sample;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Map extends Mapper<Object, Text, Text, IntWritable> {

	private final static IntWritable one = new IntWritable(1);

	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		try {
			if (value.toString().matches("(https)://.*"))
				context.write(value, one);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
