/* (C)2021 */
package com.neighborhood.aka.lapalce.hackathon.watermark

import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.streaming.runtime.operators.TimestampsAndWatermarksOperator

class WatermarkAlignOperator[T](
    watermarkStrategy: WatermarkStrategy[T],
    emitProgressiveWatermarks: Boolean
) extends TimestampsAndWatermarksOperator[T](
      watermarkStrategy,
      emitProgressiveWatermarks
    ) {}
