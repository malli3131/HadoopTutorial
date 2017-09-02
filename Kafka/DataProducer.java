package de.carbook.realtime.lkl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.bson.Document;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

public class LKLDemoProducerCarA {
	public static void main(String[] args) throws IOException {

		Map<String, String> vehicleMap = new HashMap<String, String>();
		vehicleMap.put("2606472d-461c-4432-9fd4-200ef7805fc3", "16d5afd8-ac33-4b87-9091-0572ee3e94e2");

		Map<String, String> vehicleDevice = new HashMap<String, String>();
		vehicleDevice.put("2606472d-461c-4432-9fd4-200ef7805fc3", "66104021669653");

		Properties props = new Properties();
		props.put("bootstrap.servers", "192.168.2.151:9092,192.168.2.152:9092,192.168.2.153:9092");
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		@SuppressWarnings("resource")
		KafkaProducer<String, String> producer = new KafkaProducer<String, String>(props);
		MongoClient mongo = new MongoClient(new MongoClientURI(
				"mongodb://carbook:carbook#123#@192.168.2.200:27017,192.168.2.201:27017,192.168.2.202:27017/carbook.triptransaction?authSource=admin"));
		MongoDatabase database = mongo.getDatabase("carbook");
		MongoCollection<Document> coll = database.getCollection("triptransaction");
		// String queryJson = "{ $and: [ { vehicleId: { $in:
		// ['2606472d-461c-4432-9fd4-200ef7805fc3',
		// 'afd6cf19-67b2-4401-9246-d835532ae64d','be82050b-4966-4518-8c2d-d01061257d3f']
		// } }, { trxType: \"TRACKED\" } ] }";
		String queryJson = "{ vehicleId: '2606472d-461c-4432-9fd4-200ef7805fc3' , trxType: 'TRACKED' }";
		Document fltQuery = Document.parse(queryJson);
		System.out.println(fltQuery);
		FindIterable<Document> docs = coll.find(fltQuery);
		List<String> datapoints = new ArrayList<String>();
		String vehicleId = "";
		String deviceId = "";
		for (Document doc : docs) {
			@SuppressWarnings("unchecked")
			List<Document> waypoints = (List<Document>) doc.get("waypoints");
			for (Document point : waypoints) {
				Document document = (Document) point.get("geopoint");
				Date date = (Date) point.get("date");
				vehicleId = vehicleMap.get(doc.get("vehicleId"));
				deviceId = vehicleDevice.get(doc.get("vehicleId"));
				String record = deviceId + "," + vehicleId + "," + document.get("lat") + "," + document.get("lon") + ","
						+ point.get("speed") + "," + date.getTime();
				datapoints.add(record);
			}
		}
		System.out.println(datapoints.size());
		mongo.close();
		int i = 0;
		while (true) {
			if (i == 0) {
				for (String record : datapoints) {
					try {
						ProducerRecord<String, String> producerRecord = new ProducerRecord<String, String>(
								"carbook-obd", deviceId, record);
						System.out.println("Forward: " + record);
						producer.send(producerRecord, new DemoCallback());
						Thread.sleep(5000);
					} catch (Exception e) {
						e.printStackTrace();
					}
				}
				i = 1;
				Collections.reverse(datapoints);
			} else {
				for (String record : datapoints) {
					try {
						ProducerRecord<String, String> producerRecord = new ProducerRecord<String, String>(
								"carbook-obd", deviceId, record);
						System.out.println("Backword: " + record);
						producer.send(producerRecord, new DemoCallback());
						Thread.sleep(5000);
					} catch (Exception e) {
						e.printStackTrace();
					}
				}
				i = 0;
				Collections.reverse(datapoints);
			}
		}
	}

	private static class DemoCallback implements Callback {

		public void onCompletion(RecordMetadata rm, Exception e) {
			if (e != null) {
				e.printStackTrace();
			}
		}
	}
}
