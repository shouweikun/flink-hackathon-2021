package com.neighborhood.aka.lapalce.hackathon.source

import com.neighborhood.aka.lapalce.hackathon.integrate.DataIntegrateKeyedCoProcessFunction
import com.neighborhood.aka.laplace.hackathon.version.{Versioned, VersionedDeserializationSchema}
import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.common.typeutils.TypeSerializer
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.table.api.TableSchema
import org.apache.flink.table.connector.ChangelogMode
import org.apache.flink.table.connector.format.DecodingFormat
import org.apache.flink.table.connector.source.ScanTableSource.ScanRuntimeProvider
import org.apache.flink.table.connector.source._
import org.apache.flink.table.data.RowData
import org.apache.flink.table.planner.plan.utils.KeySelectorUtil
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo
import org.apache.flink.table.utils.TableSchemaUtils
import SourceUtils.createSource
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.table.types.logical.RowType

class UnifiedTableSource(
                          private val bulkSource: ScanTableSource,
                          private val realtimeChangelogSource: ScanTableSource,
                          private val tableSchema: TableSchema,
                          private val decodingFormat: DecodingFormat

                            [VersionedDeserializationSchema],
                          private val fixedDelay: Long
                        ) extends ScanTableSource {


  private[source] class UnifiedDataStreamScanProvider(
                                                       bulkSourceProvider: ScanTableSource.ScanRuntimeProvider,
                                                       realtimeChangelogSourceProvider: ScanTableSource.ScanRuntimeProvider,
                                                       fixedDelay: Long,
                                                       versionTypeSer: TypeSerializer[Versioned],
                                                       changelogInputRowType: RowType,
                                                       outputRowType: RowType,
                                                       outputTypeInformation: TypeInformation[RowData]
                                                     ) extends DataStreamScanProvider {

    override def produceDataStream(streamExecutionEnvironment: StreamExecutionEnvironment): DataStream[RowData] = {

      val bulkSource = createSource(streamExecutionEnvironment, bulkSourceProvider)
      val realtimeChangelogSource = createSource(streamExecutionEnvironment, realtimeChangelogSourceProvider)
      val primaryKeys = TableSchemaUtils.getPrimaryKeyIndices(tableSchema)
      val bulkKeySelector = KeySelectorUtil.getRowDataSelector(primaryKeys, bulkSource.getTransformation.getOutputType.asInstanceOf[InternalTypeInfo[RowData]])
      val realtimeKeySelector = KeySelectorUtil.getRowDataSelector(primaryKeys, realtimeChangelogSource.getTransformation.getOutputType.asInstanceOf[InternalTypeInfo[RowData]])

      bulkSource
        .connect(realtimeChangelogSource)
        .keyBy(bulkKeySelector, realtimeKeySelector)
        .process(new DataIntegrateKeyedCoProcessFunction(fixedDelay, versionTypeSer, changelogInputRowType, outputRowType, outputTypeInformation, TypeInformation.of(classOf[Versioned])))
    }

    override def isBounded: Boolean = false
  }


  override def getChangelogMode: ChangelogMode = ???

  override def getScanRuntimeProvider(scanContext: ScanTableSource.ScanContext): ScanTableSource.ScanRuntimeProvider = {

    val dataType = tableSchema.toPhysicalRowDataType;

    val versionedDeserializationSchema = decodingFormat.createRuntimeDecoder(scanContext, dataType)
    val versionTypeSer = versionedDeserializationSchema.getVersionTypeSerializer
    val changlogOutputRowType = versionedDeserializationSchema.getActualRowType
    val outputRowType = dataType.getLogicalType.asInstanceOf[RowType]

    new UnifiedDataStreamScanProvider(
      bulkSource.getScanRuntimeProvider(scanContext),
      realtimeChangelogSource.getScanRuntimeProvider(scanContext),
      fixedDelay,
      versionTypeSer,
      changlogOutputRowType,
      outputRowType,
      InternalTypeInfo.of(outputRowType)
    )

  }

  override def copy(): DynamicTableSource = ???

  override def asSummaryString(): String = ???
}
