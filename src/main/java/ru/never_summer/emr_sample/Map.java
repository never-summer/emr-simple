package ru.never_summer.emr_sample;

import java.io.IOException;
import java.net.URL;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.archive.io.ArchiveReader;
import org.archive.io.ArchiveRecord;

public class Map extends Mapper<Object, ArchiveReader, Text, IntWritable> {
	private final static IntWritable outValOne = new IntWritable(1);
	private Text outKey = new Text();

	public void map(Object key, ArchiveReader value, Context context) throws IOException, InterruptedException {
		for (ArchiveRecord r : value) {
			try {
				if (r.getHeader().getMimetype().equals("application/http; msgtype=response")) {
					URL url = new URL(r.getHeader().getUrl().toLowerCase());
					outKey.set(url.getProtocol() + "://" + url.getHost());
					context.write(outKey, outValOne);
				}
			} catch (Exception ex) {

			}
		}
	}
}
