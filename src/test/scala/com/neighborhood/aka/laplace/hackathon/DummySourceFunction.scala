/* (C)2021 */
package com.neighborhood.aka.laplace.hackathon

import org.apache.flink.api.common.serialization.DeserializationSchema
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.source.{
  RichSourceFunction,
  SourceFunction
}
import org.apache.flink.table.data.RowData
import org.apache.flink.util.Collector

import java.util.concurrent.atomic.AtomicBoolean

class DummySourceFunction(val der: DeserializationSchema[RowData])
    extends RichSourceFunction[RowData] {

  private final val running = new AtomicBoolean(false)

  override def open(parameters: Configuration): Unit = {
    running.set(true)
  }

  override def run(
      sourceContext: SourceFunction.SourceContext[RowData]
  ): Unit = {

    val collector = new Collector[RowData] {
      override def collect(t: RowData): Unit = {
        sourceContext.collect(t)
      }

      override def close(): Unit = {}
    }

    while (running.get()) {
      der.deserialize(null, collector)
    }
  }

  override def cancel(): Unit = {
    running.set(false)
  }
}
