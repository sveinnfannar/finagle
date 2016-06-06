package com.twitter.finagle.zipkin.kafka

import java.util.Properties

import com.twitter.finagle.stats.DefaultStatsReceiver
import com.twitter.finagle.zipkin.core.SamplingTracer
import org.apache.kafka.clients.producer.{KafkaProducer, Producer}
import com.twitter.finagle.zipkin.{initialSampleRate => sampleRateFlag, kafkaBootstrapServers => bootstrapServersFlag}

object KafkaZipkinTracer {
  /**
    * @param kafkaBootstrapServers initial set of kafka brokers to connect to
    */
  private[kafka] def newProducer(kafkaBootstrapServers: String): Producer[Array[Byte], Array[Byte]] = {
    val props: Properties = new Properties
    props.put("bootstrap.servers", kafkaBootstrapServers) // Initial brokers to connect to, rest of the cluster will be discovered
    props.put("key.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer")
    props.put("acks", "1") // Ack after immediately writing to the leader
//    props.put("buffer.memory", maxBufferSize.inBytes: java.lang.Long)
    props.put("block.on.buffer.full", false: java.lang.Boolean) // Throw errors when buffer is full
    //    props.put("max.block.ms", 0: java.lang.Integer) // Throw errors when buffer is full
    props.put("retries", 0: java.lang.Integer)
//    props.put("request.timeout.ms", requestTimeout.inMilliseconds.toInt: java.lang.Integer)
    new KafkaProducer[Array[Byte], Array[Byte]](props)
  }
}

class KafkaZipkinTracer extends SamplingTracer(
  new KafkaRawZipkinTracer(
    producer = KafkaZipkinTracer.newProducer(bootstrapServersFlag()),
    topic = "zipkin", // parameterize
    statsReceiver = DefaultStatsReceiver.scope("zipkin")
  ),
  initialSampleRate = sampleRateFlag()
)
