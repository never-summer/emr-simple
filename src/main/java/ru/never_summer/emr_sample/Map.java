package ru.never_summer.emr_sample;

import java.io.IOException;
import java.net.URL;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;
import org.archive.io.ArchiveReader;
import org.archive.io.ArchiveRecord;

public class Map extends Mapper<Object, ArchiveReader, Text, LongWritable> {

	private static final Logger LOG = Logger.getLogger(Map.class);

	protected static enum MAPPERCOUNTER {
		RECORDS_IN, EXCEPTIONS
	}

	private final LongWritable outVal = new LongWritable(1);
	private Text outKey = new Text();
	// HTML_TAG_PATTERN =
	// re.compile(r'''<a\s+.*?href=['"](https?://[a-zA-Z].*?)[/'"].*?(?:</a|/)>''')
	// <a href="http://1037theloon.com/genesis-self-titled-album/"
	private static final String HTML_TAG_PATTERN = "<a\\s+.*?href=['\"](https?://[a-zA-Z].*?)[/'\"?].*?(?:</a|/)>";
	private Pattern patternTag;
	private Matcher matcherTag;

	public void map(Object key, ArchiveReader value, Context context) throws IOException, InterruptedException {
		patternTag = Pattern.compile(HTML_TAG_PATTERN);
		for (ArchiveRecord r : value) {
			try {
				if (r.getHeader().getMimetype().equals("application/http; msgtype=response")) {
					// Convenience function that reads the full message into a
					// raw byte array
					byte[] rawData = IOUtils.toByteArray(r, r.available());
					String content = new String(rawData);
					// The HTTP header gives us valuable information about what
					// was received during the request
					String headerText = content.substring(0, content.indexOf("\r\n\r\n"));

					// In our task, we're only interested in text/html, so we
					// can be a little lax
					// TODO: Proper HTTP header parsing + don't trust headers
					if (headerText.contains("Content-Type: text/html")) {
						context.getCounter(MAPPERCOUNTER.RECORDS_IN).increment(1);
						// Only extract the body of the HTTP response when
						// necessary
						// Due to the way strings work in Java, we don't use any
						// more memory than before
						String body = content.substring(content.indexOf("\r\n\r\n") + 4);
						// Process all the matched HTML tags found in the body
						// of the document
						matcherTag = patternTag.matcher(body);
						while (matcherTag.find()) {
							String tagName = matcherTag.group(1);
							outKey.set(tagName.toLowerCase());
							context.write(outKey, outVal);
						}
					}
				}
			} catch (Exception ex) {
				LOG.error("Caught Exception:", ex);
				context.getCounter(MAPPERCOUNTER.EXCEPTIONS).increment(1);
			}
		}
	}
}