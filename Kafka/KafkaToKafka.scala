package com.widas.spark.streaming

import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.kafka010.ConsumerStrategies
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies

import java.util.Properties
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import com.twitter.bijection.Injection
import com.twitter.bijection.avro.GenericAvroCodecs

object KafkaToKafka extends App {

  val conf = new SparkConf().setMaster("local[*]").setAppName("Kafka to Cassandra data Ingestion")
  val sc = new SparkContext(conf)
  val ssc = new StreamingContext(sc, Seconds(10))

  val props = new Properties()
  props.put("bootstrap.servers", "localhost:9092")
  props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
  props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
  val messageProducer: KafkaProducer[String, String] = new KafkaProducer(props)
  val producer = sc.broadcast(messageProducer)

  val USER_SCHEMA = "{\"type\": \"record\",\n" + "\"name\": \"Person\",\n" + "\"fields\": [\n" + "{\"name\": \"name\", \"type\": \"string\"},\n" + "{\"name\": \"place\", \"type\": \"string\"},\n" + "{\"name\": \"age\", \"type\": \"int\"},\n" + "]\n" + "}";

  val kafkaParams = Map[String, String]("bootstrap.servers" -> "ubuntu:9092", "key.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer", "value.deserializer" -> "org.apache.kafka.common.serialization.ByteArrayDeserializer", "group.id" -> "mygroup")
  val topics = Set("widas")

  val inputKafkaStream = KafkaUtils.createDirectStream(ssc, LocationStrategies.PreferConsistent, ConsumerStrategies.Subscribe[String, Array[Byte]](topics, kafkaParams))
  val kafkaData = inputKafkaStream.transform(rdd => rdd.map { avroRecord =>
    val parser = new Schema.Parser()
    val schema = parser.parse(USER_SCHEMA)
    val recordInjection: Injection[GenericRecord, Array[Byte]] = GenericAvroCodecs.toBinary(schema)
    val record = recordInjection.invert(avroRecord.value())
    val myRecord = record.get
    (myRecord.get("name").toString())
  })
  kafkaData.foreachRDD(rdd => {
    rdd.map { record =>
      val parser = new Schema.Parser()
      val schema = parser.parse(USER_SCHEMA)
      val producerRecord: ProducerRecord[String, String] = new ProducerRecord("mytopic", "Tracker", record);
      producer.value.send(producerRecord);
    }
  })

  ssc.start()
  ssc.awaitTermination()
}
