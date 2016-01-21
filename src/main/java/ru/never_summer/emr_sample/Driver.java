package ru.never_summer.emr_sample;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.net.URLConnection;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.zip.GZIPInputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.Logger;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.iterable.S3Objects;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;

public class Driver {
	private static final Logger LOG = Logger.getLogger(Driver.class);
	final static String awsPublic = "s3://aws-publicdatasets/";
	final static int threadCount = 20;
	final static String S3 = "s3://";

	public static void main(String[] args) throws Exception {
		if (args.length <= 6) {
			System.out.println("Usage:<inDir> <outDir> <bucketName> <myQueueUrl> <region> <accessKey> <secretKey>");
			System.out.println("args:" + Arrays.toString(args));
		}

		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "warc file count response http(s)");
		job.setJarByClass(Driver.class);
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		job.setInputFormatClass(WARCFileInputFormat.class);
		// add input paths for job
		// FileInputFormat.addInputPath(job, new Path(args[0]));
		addInputPaths(job, args[0]);
		String outPath = args[1];
		String bucketName = args[2];
		FileOutputFormat.setOutputPath(job, new Path(S3 + bucketName + "/" + outPath));

		if (job.waitForCompletion(true)) {
			System.out.println("Job mapReduce succes");

			String queueName = args[3];
			String region = args[4];
			String accessKey = args[5];
			String secretKey = args[6];
			System.out.println("Start sqs, queueName:" + queueName + ", region:" + region + ", accessKey:" + accessKey
					+ ", secretKey:" + secretKey);
			ArrayList<String> listFiles = new ArrayList<String>();
			ConcurrentLinkedQueue<String> listMessages = new ConcurrentLinkedQueue<String>();
			AWSCredentials credentials = null;
			try {
				credentials = new BasicAWSCredentials(accessKey, secretKey);
				AmazonS3Client s3 = new AmazonS3Client(credentials);
				Region reg = Region.getRegion(Regions.fromName(region));
				s3.setRegion(reg);
				//
				listFiles = getListFiles(outPath, s3, bucketName, outPath);
				listMessages = getListLine(listFiles, s3, bucketName);

				// start 20 thread to send message for sqs
				ExecutorService executor = Executors.newFixedThreadPool(threadCount);
				for (int i = 0; i < threadCount; i++) {
					Runnable worker = new TextMessageSqsSender(queueName, reg, accessKey, secretKey, listMessages);
					executor.execute(worker);
				}
				executor.shutdown();
				while (!executor.isTerminated()) {
					// sleep 10 sec
					Thread.sleep(10000);
				}
				System.out.println("Job succes");
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		} else {
			System.err.println("Job mapReduce error");
		}
	}

	private static ConcurrentLinkedQueue<String> getListLine(ArrayList<String> listFiles, AmazonS3Client s3,
			String bucketName) throws IOException {

		ConcurrentLinkedQueue<String> listLines = new ConcurrentLinkedQueue<String>();
		for (String path : listFiles) {
			S3Object s3object = s3.getObject(new GetObjectRequest(bucketName, path));
			System.out.println(s3object.getObjectMetadata().getContentType());
			System.out.println(s3object.getObjectMetadata().getContentLength());

			BufferedReader reader = new BufferedReader(new InputStreamReader(s3object.getObjectContent()));
			String line;
			while ((line = reader.readLine()) != null) {
				// can copy the content locally as well
				listLines.add(line);
			}
		}
		return listLines;
	}

	private static ArrayList<String> getListFiles(String srcPath, AmazonS3Client s3, String bucketName, String outDir)
			throws IOException {
		System.out.println("Start list files, path:" + srcPath);
		ArrayList<String> listFiles = new ArrayList<String>();
		for (S3ObjectSummary summary : S3Objects.withPrefix(s3, bucketName, outDir)) {
			String file = summary.getKey();
			System.out.printf("Object with key '%s'\n", file);
			listFiles.add(file);
		}
		return listFiles;
	}

	private static void addInputPaths(Job job, String urlFeil) throws IOException {
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
			System.out.printf("addInputPath:" + awsPublic + line);
		}

	}
}
