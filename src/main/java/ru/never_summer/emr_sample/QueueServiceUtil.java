package ru.never_summer.emr_sample;

import java.util.ArrayList;

import org.apache.log4j.Logger;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.SendMessageRequest;

/**
 * This sample demonstrates how to make basic requests to Amazon SQS using the
 * AWS SDK for Java.
 * <p>
 * <b>Prerequisites:</b> You must have a valid Amazon Web Services developer
 * account, and be signed up to use Amazon SQS. For more information on Amazon
 * SQS, see http://aws.amazon.com/sqs.
 * <p>
 * Fill in your AWS access credentials in the provided credentials file
 * template, and be sure to move the file to the default location
 * (~/.aws/credentials) where the sample code will load the credentials from.
 * <p>
 * <b>WARNING:</b> To avoid accidental leakage of your credentials, DO NOT keep
 * the credentials file in your source directory.
 */
public class QueueServiceUtil {
	private static final Logger LOG = Logger.getLogger(QueueServiceUtil.class);

	public static void process(String myQueueUrl, String region, AWSCredentials credentials,
			ArrayList<String> listLines) throws Exception {

		AmazonSQS sqs = new AmazonSQSClient(credentials);
		Region regionName = Region.getRegion(Regions.fromName(region));
		sqs.setRegion(regionName);

		LOG.info("===========================================");
		LOG.info("Getting Started with Amazon SQS");
		LOG.info("===========================================\n");

		try {

			// Send a message
			LOG.info("Sending a message to " + myQueueUrl);
			for (String msg : listLines)
				sqs.sendMessage(new SendMessageRequest(myQueueUrl, msg));

		} catch (AmazonServiceException ase) {
			LOG.error("Caught an AmazonServiceException, which means your request made it "
					+ "to Amazon SQS, but was rejected with an error response for some reason.");
			LOG.error("Error Message:    " + ase.getMessage());
			LOG.error("HTTP Status Code: " + ase.getStatusCode());
			LOG.error("AWS Error Code:   " + ase.getErrorCode());
			LOG.error("Error Type:       " + ase.getErrorType());
			LOG.error("Request ID:       " + ase.getRequestId());
		} catch (AmazonClientException ace) {
			LOG.error("Caught an AmazonClientException, which means the client encountered "
					+ "a serious internal problem while trying to communicate with SQS, such as not "
					+ "being able to access the network.");
			LOG.error("Error Message: " + ace.getMessage());
		}
	}
}