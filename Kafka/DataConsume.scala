import java.util.Arrays
import java.util.Properties

import scala.collection.JavaConversions.iterableAsScalaIterable

import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.clients.consumer.KafkaConsumer

import com.twitter.bijection.Injection
import com.twitter.bijection.avro.GenericAvroCodecs

object DataConsume extends App {
  val props = new Properties();
  val USER_SCHEMA = "{" + "\"type\":\"record\"," + "\"name\":\"person\"," + "\"fields\":[" + "  { \"name\":\"sno\", \"type\":\"int\" }," + "  { \"name\":\"name\", \"type\":\"string\" }," + "  { \"name\":\"age\", \"type\":\"int\" }" + "]}"
  props.put("bootstrap.servers", "localhost:9092");
  props.put("group.id", "mygroup");
  props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
  props.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");

  val parser = new Schema.Parser()
  val schema = parser.parse(USER_SCHEMA)

  val recordInjection: Injection[GenericRecord, Array[Byte]] = GenericAvroCodecs.toBinary(schema);

  val consumer: KafkaConsumer[String, Array[Byte]] = new KafkaConsumer(props);
  consumer.subscribe(Arrays.asList("widas"));

  val running = true;
  while (running) {
    val records = consumer.poll(100);
    for (record <- records) {
      val datarecord = recordInjection.invert(record.value())
      println(datarecord);
    }
  }
  consumer.close();
}
