package ru.never_summer.emr_sample;

import java.util.concurrent.ConcurrentLinkedQueue;

import javax.jms.JMSException;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;

import com.amazon.sqs.javamessaging.AmazonSQSMessagingClientWrapper;
import com.amazon.sqs.javamessaging.SQSConnection;
import com.amazon.sqs.javamessaging.SQSConnectionFactory;
import com.amazonaws.regions.Region;

public class TextMessageSqsSender implements Runnable {
	private String myQueueName;
	private Region region;
	private String awsAccessKeyId;
	private String awsSecretKey;
	private ConcurrentLinkedQueue<String> listMessages;

	TextMessageSqsSender(String myQueueName, Region region, String awsAccessKeyId, String awsSecretKey,
			ConcurrentLinkedQueue<String> listMessages) {
		this.myQueueName = myQueueName;
		this.region = region;
		this.awsAccessKeyId = awsAccessKeyId;
		this.awsSecretKey = awsSecretKey;
		this.listMessages = listMessages;
	}

	@Override
	public void run() {
		// Create the connection factory
		try {
			SQSConnectionFactory connectionFactory = SQSConnectionFactory.builder().withRegion(region).build();

			// Create the connection
			SQSConnection connection = connectionFactory.createConnection(awsAccessKeyId, awsSecretKey);

			// Create the queue if needed
			ensureQueueExists(connection, myQueueName);

			// Create the session
			Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
			MessageProducer producer = session.createProducer(session.createQueue(myQueueName));

			sendMessages(session, producer, listMessages);

			// Close the connection. This will close the session automatically
			connection.close();
			System.out.println("Connection closed");
		} catch (JMSException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	private static void sendMessages(Session session, MessageProducer producer,
			ConcurrentLinkedQueue<String> listMessages) throws JMSException {
		String msg = null;
		while (!listMessages.isEmpty()) {
			msg = listMessages.poll();
			if (msg != null) {
				TextMessage message = session.createTextMessage(msg);
				producer.send(message);
			}
		}

	}

	public static void ensureQueueExists(SQSConnection connection, String queueName) throws JMSException {
		AmazonSQSMessagingClientWrapper client = connection.getWrappedAmazonSQSClient();

		/**
		 * For most cases this could be done with just a createQueue call, but
		 * GetQueueUrl (called by queueExists) is a faster operation for the
		 * common case where the queue already exists. Also many users and roles
		 * have permission to call GetQueueUrl but do not have permission to
		 * call CreateQueue.
		 */
		if (!client.queueExists(queueName)) {
			client.createQueue(queueName);
		}
	}

}