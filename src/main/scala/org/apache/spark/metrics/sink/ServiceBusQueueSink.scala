/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.metrics.sink

import com.codahale.metrics.{CsvReporter, MetricRegistry}

import java.util.{Locale, Properties}
import java.util.concurrent.TimeUnit

import com.elastacloud.spark.QueueReporter
import org.apache.spark.metrics.MetricsSystem

class ServiceBusQueueSink(val property: Properties, val registry: MetricRegistry) extends Sink {
  val QUEUE_KEY_PERIOD = "period"
  val QUEUE_KEY_UNIT = "unit"
  val QUEUE_KEY_CONNECTION = "connection"
  val QUEUE_KEY_QUEUE = "queue"

  val QUEUE_DEFAULT_PERIOD = 10
  val QUEUE_DEFAULT_UNIT = "SECONDS"
  val QUEUE_DEFAULT_CONNECTION = null
  val QUEUE_DEFAULT_QUEUE = null

  val pollPeriod = Option(property.getProperty(QUEUE_KEY_PERIOD)) match {
    case Some(s) => s.toInt
    case None => QUEUE_DEFAULT_PERIOD
  }

  val pollUnit = Option(property.getProperty(QUEUE_KEY_UNIT)) match {
    case Some(s) => TimeUnit.valueOf(s.toUpperCase())
    case None => TimeUnit.valueOf(QUEUE_DEFAULT_UNIT)
  }

  val connectionStr = Option(property.getProperty(QUEUE_KEY_CONNECTION)) match {
    case Some(s) => s
    case None => QUEUE_DEFAULT_CONNECTION
  }

  val queueNameStr = Option(property.getProperty(QUEUE_KEY_QUEUE)) match {
    case Some(s) => s.toLowerCase()
    case None => QUEUE_DEFAULT_QUEUE
  }

  MetricsSystem.checkMinimalPollingPeriod(pollUnit, pollPeriod)

  val reporter: QueueReporter = QueueReporter.forRegistry(registry)
    .convertDurationsTo(TimeUnit.MILLISECONDS)
    .convertRatesTo(TimeUnit.SECONDS)
    .build(connectionStr, queueNameStr)

  override def start() {
    reporter.start(pollPeriod, pollUnit)
  }

  override def stop() {
    reporter.stop()
  }
}
