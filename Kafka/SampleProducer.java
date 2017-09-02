package naga.kafka.producer;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

public class SampleProducer {
	public static void main(String[] args) throws IOException {
		Properties props = new Properties();
		props.put("bootstrap.servers", "localhost:9092");
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		KafkaProducer<String, String> producer = new KafkaProducer<String, String>(props);
		BufferedReader reader = new BufferedReader(new FileReader(new File("/Users/nagainelu/bigdata/jobs/stocks")));
		String line = "";
		long start = System.currentTimeMillis();
		while ((line = reader.readLine()) != null) {
			String columns[] = line.split("\\t");
			if (columns.length == 9) {
				ProducerRecord<String, String> record = new ProducerRecord<String, String>("stocks", columns[1],
						line);
				try {
					producer.send(record, new DemoCallback());
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}
		reader.close();
		producer.close();
		long end = System.currentTimeMillis();
		System.out.println("Elapsed Time: " + (end - start) + " ms");
	}

	private static class DemoCallback implements Callback {

		@Override
		public void onCompletion(RecordMetadata rm, Exception e) {
			if (e != null) {
				e.printStackTrace();
			}
		}
	}
}
