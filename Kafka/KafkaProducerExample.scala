package com.widas.kafka

import java.util.Properties

import scala.util.Random

import org.apache.kafka.clients.producer.KafkaProducer
import scala.collection.immutable.Range
import org.apache.kafka.clients.producer.ProducerRecord
import java.util.Date

object KafkaProducerExample extends App {
  val events = args(0).toInt
  val random = new Random()

  val topic = args(1)
  val brokers = args(2)
  val properties = new Properties()
  properties.put("bootstrap.servers", brokers)
  properties.put("client.id", "KafkaProducerExample")
  properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

  val producer = new KafkaProducer[String, String](properties)

  val time = System.currentTimeMillis()
  for (event <- Range(0, events)) {
    val runtime = new Date().getTime()
    val ip = "192.168.1." + random.nextInt(255)
    val msg = runtime + "," + event + "," + ip + "," + "www.widas.in"
    val record = new ProducerRecord[String, String](topic, ip, msg)
    producer.send(record)
    Thread.sleep(1)
  }
  println("sent per second: " + events * 1000 / (System.currentTimeMillis() - time))
  producer.close()
}
