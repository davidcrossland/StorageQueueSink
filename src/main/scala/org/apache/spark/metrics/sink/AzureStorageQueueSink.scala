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

class AzureStorageQueueSink(val property: Properties, val registry: MetricRegistry) extends Sink {
  val QUEUE_KEY_PERIOD = "period"
  val QUEUE_KEY_UNIT = "unit"
  val QUEUE_KEY_URI = "uri"
  val QUEUE_KEY_SAS = "sas"
  val QUEUE_KEY_QUEUE = "queue"
  val CLUSTER_NAME = "clustername"

  val QUEUE_DEFAULT_PERIOD = 10
  val QUEUE_DEFAULT_UNIT = "SECONDS"
  val QUEUE_DEFAULT_CONNECTION = null
  val QUEUE_DEFAULT_QUEUE = null
  val CLUSTER_NAME_DEFAULT = null

  val pollPeriod = Option(property.getProperty(QUEUE_KEY_PERIOD)) match {
    case Some(s) => s.toInt
    case None => QUEUE_DEFAULT_PERIOD
  }

  val pollUnit = Option(property.getProperty(QUEUE_KEY_UNIT)) match {
    case Some(s) => TimeUnit.valueOf(s.toUpperCase())
    case None => TimeUnit.valueOf(QUEUE_DEFAULT_UNIT)
  }

  val baseUri = Option(property.getProperty(QUEUE_KEY_URI)) match {
    case Some(s) => s
    case None => QUEUE_DEFAULT_CONNECTION
  }

  val sharedAccessSignature = Option(property.getProperty(QUEUE_KEY_SAS)) match {
    case Some(s) => s
    case None => QUEUE_KEY_SAS
  }

  val queueNameStr = Option(property.getProperty(QUEUE_KEY_QUEUE)) match {
    case Some(s) => s.toLowerCase()
    case None => QUEUE_DEFAULT_QUEUE
  }

  val clusterName = Option(property.getProperty(CLUSTER_NAME)) match {
    case Some(s) => s
    case None => CLUSTER_NAME_DEFAULT
  }

  MetricsSystem.checkMinimalPollingPeriod(pollUnit, pollPeriod)

  val reporter: QueueReporter = QueueReporter.forRegistry(registry)
    .convertDurationsTo(TimeUnit.MILLISECONDS)
    .convertRatesTo(TimeUnit.SECONDS)
    .build(baseUri, queueNameStr, sharedAccessSignature, clusterName)

  override def start() {
    println("Starting metrics ...")
    reporter.start(pollPeriod, pollUnit)
  }

  override def stop() {
    println("Stopping metrics ...")
    reporter.stop()
  }
}
