package ru.never_summer.emr_sample;

import java.io.IOException;
import java.net.URL;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Map extends Mapper<Object, Text, Text, IntWritable> {
	private static final Pattern pattern = Pattern.compile("(https?|http)://*");
	private final static IntWritable one = new IntWritable(1);

	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		String tmp = value.toString();
		Matcher matcher = pattern.matcher(tmp);
		if (matcher.find()) {
			try {
				URL url = new URL(tmp);
				context.write(new Text(url.getHost()), one);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}
}
