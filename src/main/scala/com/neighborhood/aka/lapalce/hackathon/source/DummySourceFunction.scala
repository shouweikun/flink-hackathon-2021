/* (C)2021 */
package com.neighborhood.aka.lapalce.hackathon.source

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.functions.source.{
  RichSourceFunction,
  SourceFunction
}

import java.util.concurrent.atomic.AtomicBoolean

class DummySourceFunction[T] extends RichSourceFunction[T] {

  private val running = new AtomicBoolean(true)
  override def run(ctx: SourceFunction.SourceContext[T]): Unit = {
    while (running.get()) {
      Thread.sleep(50)
    }
  }
  override def cancel(): Unit = running.set(false)

}
