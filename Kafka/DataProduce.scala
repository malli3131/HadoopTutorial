import java.util.Properties

import org.apache.avro.Schema
import org.apache.avro.generic.GenericData
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord

import com.twitter.bijection.Injection
import com.twitter.bijection.avro.GenericAvroCodecs

object DataProduce extends App {
  val topic = "widas"
  val USER_SCHEMA = "{" + "\"type\":\"record\"," + "\"name\":\"person\"," + "\"fields\":[" + "  { \"name\":\"sno\", \"type\":\"int\" }," + "  { \"name\":\"name\", \"type\":\"string\" }," + "  { \"name\":\"age\", \"type\":\"int\" }" + "]}"
  val props = new Properties()
  props.put("bootstrap.servers", "localhost:9092")
  props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
  props.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");

  val parser = new Schema.Parser()
  val schema = parser.parse(USER_SCHEMA)

  val recordInjection: Injection[GenericRecord, Array[Byte]] = GenericAvroCodecs.toBinary(schema);

  val messageProducer: KafkaProducer[String, Array[Byte]] = new KafkaProducer(props)
  val avroRecord = new GenericData.Record(schema);
  avroRecord.put("sno", 100)
  avroRecord.put("name", "Naga")
  avroRecord.put("age", 30)
  println(avroRecord)
  var i = 0;
  while (i < 100) {
    val dataBytes = recordInjection.apply(avroRecord);
    val producerRecord: ProducerRecord[String, Array[Byte]] = new ProducerRecord(topic, "Tracker", dataBytes);
    messageProducer.send(producerRecord);
    i = i + 1
  }
  messageProducer.close();
}
