package ru.never_summer.emr_sample;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.net.URLConnection;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.zip.GZIPInputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.Logger;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.iterable.S3Objects;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;

public class Driver {
	private static final Logger LOG = Logger.getLogger(Driver.class);

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
		FileOutputFormat.setOutputPath(job, new Path("s3://" + bucketName + "/" + outPath));

		if (job.waitForCompletion(true)) {
			System.out.println("Job mapReduce succes");

			String myQueueUrl = args[3];
			String region = args[4];
			String accessKey = args[5];
			String secretKey = args[6];
			System.out.println("Start sqs, myQueueUrl:" + myQueueUrl + ", region:" + region + ", accessKey:" + accessKey
					+ ", secretKey:" + secretKey);
			ArrayList<String> listFiles = new ArrayList<String>();
			ArrayList<String> listLines = new ArrayList<String>();
			AWSCredentials credentials = null;
			credentials = new BasicAWSCredentials(accessKey, secretKey);
			listFiles = getListFiles(outPath, credentials, bucketName, outPath);
			listLines = getListLine(listFiles, credentials, bucketName);
			QueueServiceUtil.process(myQueueUrl, region, credentials, listLines);
			System.out.println("Job succes");
		}

		// if (args.length > 1) {
		// if (args[2].equalsIgnoreCase("mapr")) {
		// System.out.println("Start only mapreduce");
		// FileOutputFormat.setOutputPath(job, new Path(outPath));
		// System.exit(job.waitForCompletion(true) ? 0 : 1);
		// } else if (args[2].equalsIgnoreCase("sqs")) {
		// if (args.length <= 6) {
		// System.out.println("Usage:<inDir> <outDir> <sqs> <myQueueUrl>
		// <region> <accessKey> <secretKey>");
		// System.out.println("args:" + Arrays.toString(args));
		// }
		// if (job.waitForCompletion(true)) {
		// String myQueueUrl = args[3];
		// String region = args[4];
		// String accessKey = args[5];
		// String secretKey = args[6];
		// System.out.println("Start sqs, myQueueUrl:" + myQueueUrl + ",
		// region:" + region + ", accessKey:"
		// + accessKey + ", secretKey:" + secretKey);
		// ArrayList<String> listFiles = new ArrayList<String>();
		// ArrayList<String> listLines = new ArrayList<String>();
		// listFiles = getListFiles(outPath);
		// listLines = getListLine(listFiles);
		// QueueServiceUtil.process(myQueueUrl, region, accessKey, secretKey,
		// listLines);
		// System.out.println("Job succes");
		// } else {
		// System.out.println("Execute job file");
		// }
		// } else {
		//
		// if (args.length <= 5) {
		// System.out.println("Usage:<inDir> <outDir> <myQueueUrl> <region>
		// <accessKey> <secretKey>");
		// System.out.println("args:" + Arrays.toString(args));
		// }
		// if (job.waitForCompletion(true)) {
		// String myQueueUrl = args[3];
		// String region = args[4];
		// String accessKey = args[5];
		// String secretKey = args[6];
		// System.out.println("Start sqs, myQueueUrl:" + myQueueUrl + ",
		// region:" + region + ", accessKey:"
		// + accessKey + ", secretKey:" + secretKey);
		// ArrayList<String> listFiles = new ArrayList<String>();
		// ArrayList<String> listLines = new ArrayList<String>();
		// listFiles = getListFiles(outPath);
		// listLines = getListLine(listFiles);
		// QueueServiceUtil.process(myQueueUrl, region, accessKey, secretKey,
		// listLines);
		// System.out.println("Job succes");
		// }
		// }
		// } else {
		// System.exit(job.waitForCompletion(true) ? 0 : 1);
		// }
	}

	private static ArrayList<String> getListLine(ArrayList<String> listFiles, AWSCredentials credentials,
			String bucketName) throws IOException {

		ArrayList<String> listLines = new ArrayList<String>();
		AmazonS3Client s3 = new AmazonS3Client(credentials);
		for (String path : listFiles) {
			S3Object s3object = s3.getObject(new GetObjectRequest(bucketName, path));
			System.out.println(s3object.getObjectMetadata().getContentType());
			System.out.println(s3object.getObjectMetadata().getContentLength());

			BufferedReader reader = new BufferedReader(new InputStreamReader(s3object.getObjectContent()));
			String line;
			while ((line = reader.readLine()) != null) {
				// can copy the content locally as well
				// using a buffered writer
				// System.out.println(line);
				listLines.add(line);
			}
		}
		// ArrayList<String> listLines = new ArrayList<String>();
		// for (String path : listFiles) {
		// Path pt = new Path(path);
		// FileSystem fs = FileSystem.get(new Configuration());
		// BufferedReader br = new BufferedReader(new
		// InputStreamReader(fs.open(pt)));
		// String line;
		// line = br.readLine();
		// while (line != null) {
		// listLines.add(line);
		// line = br.readLine();
		// }
		// }
		return listLines;
	}

	private static ArrayList<String> getListFiles(String srcPath, AWSCredentials credentials, String bucketName,
			String outDir) throws IOException {
		System.out.println("Start list files, path:" + srcPath);
		AmazonS3Client s3 = new AmazonS3Client(credentials);
		ArrayList<String> listFiles = new ArrayList<String>();
		for (S3ObjectSummary summary : S3Objects.withPrefix(s3, bucketName, outDir)) {
			String file = summary.getKey();
			System.out.printf("Object with key '%s'\n", file);
			listFiles.add(file);
		}

		// FileSystem fs = FileSystem.get(new Configuration());
		//
		// RemoteIterator<LocatedFileStatus> rmIterator =
		// fs.listLocatedStatus(new Path(srcPath));
		// while (rmIterator.hasNext()) {
		// Path path = rmIterator.next().getPath();
		// if (fs.isFile(path)) {
		// System.out.println("Add file:" + path);
		// listFiles.add(String.valueOf(path));
		// }
		// }
		return listFiles;
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
			LOG.info("addInputPath:" + awsPublic + line);
		}

	}
}
