package com.neighborhood.aka.lapalce.hackathon.source

import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.table.connector.source.ScanTableSource.ScanRuntimeProvider
import org.apache.flink.table.connector.source.{DataStreamScanProvider, SourceFunctionProvider, SourceProvider}
import org.apache.flink.table.data.RowData

object SourceUtils {

  private[source] def createSource(env: StreamExecutionEnvironment, runtimeProvider: ScanRuntimeProvider) = {
    runtimeProvider match {
      case provider: SourceFunctionProvider =>
        val sourceFunction = provider.createSourceFunction()
        env
          .addSource(sourceFunction)
      case provider: SourceProvider =>
        val strategy: WatermarkStrategy[RowData] = WatermarkStrategy.noWatermarks()
        env.fromSource(provider.createSource(), strategy, "")
      case provider: DataStreamScanProvider =>
        provider.produceDataStream(env)
    }
  }
}
