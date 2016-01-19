package ru.never_summer.emr_sample;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.net.URLConnection;
import java.util.zip.GZIPInputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Driver {
	public static void main(String[] args) throws Exception {
		if (args.length < 1) {
			System.out.println("Usage: <inDir> <outDir> and optional<countReducer>");
		}
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "warc file count response http(s)");
		job.setJarByClass(Driver.class);
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		job.setInputFormatClass(WARCFileInputFormat.class);
		// add input paths for job
		// FileInputFormat.addInputPath(job, new Path(args[0]));
		addInputPaths(job, args[0]);
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

	private static void addInputPaths(Job job, String urlFeil) throws IOException {
		final String awsPublic = "s3://aws-publicdatasets/";
		URL url = new URL(urlFeil);
		URLConnection connection = url.openConnection();
		InputStream is = connection.getInputStream();
		GZIPInputStream gzis = new GZIPInputStream(is);
		InputStreamReader xover = new InputStreamReader(gzis);
		BufferedReader br = new BufferedReader(xover);

		String line;
		// Now read lines of text: the BufferedReader puts them in lines,
		// the InputStreamReader does Unicode conversion, and the
		// GZipInputStream "gunzip"s the data from the FileInputStream.
		while ((line = br.readLine()) != null) {
			FileInputFormat.addInputPath(job, new Path(awsPublic + line));
			System.out.println("addInputPath:" + awsPublic + line);
		}

	}
}
