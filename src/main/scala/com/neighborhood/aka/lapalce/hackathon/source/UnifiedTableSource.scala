/* (C)2021 */
package com.neighborhood.aka.lapalce.hackathon.source

import com.neighborhood.aka.lapalce.hackathon.integrate.{
  DataIntegrateKeyedCoProcessFunction,
  SpecializedKeyedCoProcessOperator
}
import com.neighborhood.aka.lapalce.hackathon.source.SourceUtils.createSource
import com.neighborhood.aka.laplace.hackathon.VersionedDeserializationSchema
import com.neighborhood.aka.laplace.hackathon.version.Versioned
import com.neighborhood.aka.laplace.hackathon.watermark.AlignedTimestampsAndWatermarksOperatorFactory
import org.apache.flink.api.common.eventtime._
import org.apache.flink.api.common.functions.FilterFunction
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.common.typeutils.TypeSerializer
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.table.api.TableSchema
import org.apache.flink.table.connector.ChangelogMode
import org.apache.flink.table.connector.format.DecodingFormat
import org.apache.flink.table.connector.source._
import org.apache.flink.table.data.RowData
import org.apache.flink.table.planner.plan.utils.KeySelectorUtil
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo
import org.apache.flink.table.types.logical.RowType
import org.apache.flink.table.utils.TableSchemaUtils
import org.apache.flink.types.RowKind

import javax.annotation.Nullable

class UnifiedTableSource(
    @Nullable private val bulkSource: ScanTableSource,
    private val realtimeChangelogSource: ScanTableSource,
    private val tableSchema: TableSchema,
    private val decodingFormat: DecodingFormat[VersionedDeserializationSchema],
    private val fixedDelay: Long,
    private val bulkParallelism: Option[Int],
    private val changelogParallelism: Int,
    private val watermarkAlign: Boolean,
    private val disableBulk: Boolean
) extends ScanTableSource {

  private[source] class UnifiedDataStreamScanProvider(
      @Nullable bulkSourceProvider: ScanTableSource.ScanRuntimeProvider,
      realtimeChangelogSourceProvider: ScanTableSource.ScanRuntimeProvider,
      fixedDelay: Long,
      versionTypeSer: TypeSerializer[Versioned],
      changelogInputRowType: RowType,
      outputRowType: RowType,
      outputTypeInformation: TypeInformation[RowData]
  ) extends DataStreamScanProvider {

    override def produceDataStream(
        streamExecutionEnvironment: StreamExecutionEnvironment
    ): DataStream[RowData] = {

      val bulkSource = if (disableBulk) {
        val source =
          streamExecutionEnvironment.addSource(new DummySourceFunction[RowData])
        source.getTransformation.setOutputType(outputTypeInformation)
        source
      } else {
        createSource(
          streamExecutionEnvironment,
          bulkSourceProvider,
          Option(outputTypeInformation)
        )
      }

      val realtimeChangelogSource = createSource(
        streamExecutionEnvironment,
        realtimeChangelogSourceProvider,
        Option(InternalTypeInfo.of(changelogInputRowType))
      )
      val primaryKeys = TableSchemaUtils.getPrimaryKeyIndices(tableSchema)
      val bulkKeySelector = KeySelectorUtil.getRowDataSelector(
        primaryKeys,
        bulkSource.getTransformation.getOutputType
          .asInstanceOf[InternalTypeInfo[RowData]]
      )
      val realtimeKeySelector = KeySelectorUtil.getRowDataSelector(
        primaryKeys,
        realtimeChangelogSource.getTransformation.getOutputType
          .asInstanceOf[InternalTypeInfo[RowData]]
      )

      val watermarkGeneratorSupplier = new WatermarkGeneratorSupplier[RowData] {

        val vtser = versionTypeSer

        override def createWatermarkGenerator(
            context: WatermarkGeneratorSupplier.Context
        ): WatermarkGenerator[RowData] = {
          new WatermarkGenerator[RowData] {

            private var currTs: Long = Long.MinValue

            override def onEvent(
                t: RowData,
                l: Long,
                watermarkOutput: WatermarkOutput
            ): Unit = {
              val ts = t
                .getRawValue(t.getArity - 1)
                .toObject(vtser)
                .getGeneratedTs
              if (currTs < ts) {
                currTs = ts
                watermarkOutput.emitWatermark(new Watermark(ts))
              }

            }

            override def onPeriodicEmit(
                watermarkOutput: WatermarkOutput
            ): Unit = {
              watermarkOutput.emitWatermark(new Watermark(currTs))
            }
          }
        }
      }

      realtimeChangelogSource.getTransformation.setParallelism(
        changelogParallelism
      )
      bulkParallelism.foreach(bulkSource.getTransformation.setParallelism)

      val watermarked = if (watermarkAlign) {
        realtimeChangelogSource.transform(
          "watermarkAlign",
          realtimeChangelogSource.getTransformation.getOutputType,
          new AlignedTimestampsAndWatermarksOperatorFactory[RowData, RowData](
            WatermarkStrategy.forGenerator(watermarkGeneratorSupplier)
          )
        )
      } else {
        realtimeChangelogSource
          .assignTimestampsAndWatermarks(
            WatermarkStrategy.forGenerator(watermarkGeneratorSupplier)
          )
      }

      val heartbeatFilter = watermarked.filter(new FilterFunction[RowData] {

        final val typeSer = versionTypeSer

        override def filter(value: RowData): Boolean = {
          !value
            .getRawValue(value.getArity - 1)
            .toObject(typeSer)
            .isHeartbeat
        }
      })

      heartbeatFilter.getTransformation.setOutputType(
        InternalTypeInfo.of(changelogInputRowType)
      )

      val process = bulkSource
        .connect(
          heartbeatFilter
        )
        .keyBy(bulkKeySelector, realtimeKeySelector)
        .transform(
          "dataIntegrate",
          outputTypeInformation,
          new SpecializedKeyedCoProcessOperator(
            new DataIntegrateKeyedCoProcessFunction(
              fixedDelay,
              versionTypeSer,
              changelogInputRowType,
              outputRowType,
              outputTypeInformation,
              TypeInformation.of(classOf[Versioned]),
              watermarkAlign
            )
          )
        )

      process.setParallelism(streamExecutionEnvironment.getParallelism)

      process
    }

    override def isBounded: Boolean = false
  }

  override def getChangelogMode: ChangelogMode =
    ChangelogMode
      .newBuilder()
      .addContainedKind(RowKind.DELETE)
      .addContainedKind(RowKind.INSERT)
      .addContainedKind(RowKind.UPDATE_AFTER)
      .addContainedKind(RowKind.UPDATE_BEFORE)
      .build()

  override def getScanRuntimeProvider(
      scanContext: ScanTableSource.ScanContext
  ): ScanTableSource.ScanRuntimeProvider = {

    val dataType = tableSchema.toPhysicalRowDataType;

    val versionedDeserializationSchema =
      decodingFormat.createRuntimeDecoder(scanContext, dataType)
    val versionTypeSer = versionedDeserializationSchema.getVersionTypeSerializer
    val changlogOutputRowType = versionedDeserializationSchema.getActualRowType
    val outputRowType = dataType.getLogicalType.asInstanceOf[RowType]

    new UnifiedDataStreamScanProvider(
      Option(bulkSource).map(_.getScanRuntimeProvider(scanContext)).orNull,
      realtimeChangelogSource.getScanRuntimeProvider(scanContext),
      fixedDelay,
      versionTypeSer,
      changlogOutputRowType,
      outputRowType,
      InternalTypeInfo.of(outputRowType)
    )

  }

  override def copy(): DynamicTableSource = new UnifiedTableSource(
    bulkSource,
    realtimeChangelogSource,
    tableSchema,
    decodingFormat,
    fixedDelay,
    bulkParallelism,
    changelogParallelism,
    watermarkAlign,
    disableBulk
  )

  override def asSummaryString(): String = "unified source"
}
