package com.twitter.finagle.zipkin

import com.twitter.app.GlobalFlag
import com.twitter.finagle.zipkin.core.Sampler

object kafkaBootstrapServers extends GlobalFlag[String](
  "localhost:9092",
  "Initial set of kafka servers to connect to, rest of cluster will be discovered (comma separated)")

object initialSampleRate extends GlobalFlag[Float](
  Sampler.DefaultSampleRate,
  "Initial sample rate")
