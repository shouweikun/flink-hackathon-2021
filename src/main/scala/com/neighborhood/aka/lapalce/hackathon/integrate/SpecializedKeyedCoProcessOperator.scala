/* (C)2021 */
package com.neighborhood.aka.lapalce.hackathon.integrate

import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction
import org.apache.flink.streaming.api.operators.AbstractStreamOperator
import org.apache.flink.streaming.api.operators.co.KeyedCoProcessOperator

/**
  *
  * make [[AbstractStreamOperator#processWatermark1()]] silent
  *
  * @param keyedCoProcessFunction
  * @tparam K
  * @tparam IN1
  * @tparam IN2
  * @tparam OUT
  */
class SpecializedKeyedCoProcessOperator[K, IN1, IN2, OUT](
    keyedCoProcessFunction: KeyedCoProcessFunction[K, IN1, IN2, OUT]
) extends KeyedCoProcessOperator[K, IN1, IN2, OUT](keyedCoProcessFunction) {

  override def open(): Unit = {

    val field =
      classOf[AbstractStreamOperator[_]].getDeclaredField("input1Watermark")
    field.setAccessible(true)
    field.setLong(this, Long.MaxValue)

    super.open()

  }
}
