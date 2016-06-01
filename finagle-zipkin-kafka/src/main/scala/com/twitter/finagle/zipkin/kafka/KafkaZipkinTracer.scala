package com.twitter.finagle.zipkin.kafka

import java.util.Properties

import com.twitter.finagle.stats.DefaultStatsReceiver
import com.twitter.finagle.zipkin.core.SamplingTracer
import com.twitter.finagle.zipkin.{initialSampleRate => sampleRateFlag}
import org.apache.kafka.clients.producer.{KafkaProducer, Producer}

object KafkaZipkinTracer {
  /**
    * @param kafkaBootstrapServers initial set of kafka brokers to connect to
    */
  private[kafka] def newProducer(kafkaBootstrapServers: String): Producer[Array[Byte], Array[Byte]] = {
    val props: Properties = new Properties
    props.put("bootstrap.servers", kafkaBootstrapServers)
    props.put("metadata.broker.list", kafkaBootstrapServers) // TODO: Is this needed?
    props.put("serializer.class", "kafka.serializer.DefaultEncoder")
    props.put("producer.type", "async")
    props.put("request.required.acks", "1")
    new KafkaProducer[Array[Byte], Array[Byte]](props)
  }
}

class KafkaZipkinTracer extends SamplingTracer(
  new KafkaRawZipkinTracer(
    producer = KafkaZipkinTracer.newProducer("localhost:9092"), // TODO: Get with flag
    topic = "zipkin", // TODO: Get with flag
    statsReceiver = DefaultStatsReceiver.scope("zipkin")
  ),
  initialSampleRate = sampleRateFlag()
)
