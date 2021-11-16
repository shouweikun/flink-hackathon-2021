/* (C)2021 */
package com.neighborhood.aka.lapalce.hackathon.integrate

import com.neighborhood.aka.laplace.hackathon.util.RowDataUtils
import com.neighborhood.aka.laplace.hackathon.version.Versioned
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.common.typeinfo.{TypeInformation, Types}
import org.apache.flink.api.common.typeutils.TypeSerializer
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction
import org.apache.flink.table.api.TableConfig
import org.apache.flink.table.data.RowData
import org.apache.flink.table.data.utils.JoinedRowData
import org.apache.flink.table.planner.codegen.{
  CodeGeneratorContext,
  ProjectionCodeGenerator
}
import org.apache.flink.table.runtime.generated.Projection
import org.apache.flink.table.types.logical.RowType
import org.apache.flink.types.RowKind
import org.apache.flink.util.Collector
import org.slf4j.LoggerFactory

import java.lang.{Boolean => JBoolean, Long => JLong}

class DataIntegrateKeyedCoProcessFunction(
    private val fixedDelay: Long,
    private val versionTypeSer: TypeSerializer[Versioned],
    private val changelogInputRowType: RowType,
    private val outputRowType: RowType,
    private val outputTypeInformation: TypeInformation[RowData],
    private val versionTypeInformation: TypeInformation[Versioned]
) extends KeyedCoProcessFunction[RowData, RowData, RowData, RowData] {

  private final val logger =
    LoggerFactory.getLogger(classOf[DataIntegrateKeyedCoProcessFunction])

  private var bulkDataProcessed: ValueState[JBoolean] = _
  private var bulkData: ValueState[RowData] = _
  private var changelogVersion: ValueState[Versioned] = _
  private var registeredTime: ValueState[JLong] = _

  private var copyRowProjection: Projection[RowData, RowData] = _

  override def open(parameters: Configuration): Unit = {

    bulkDataProcessed = getRuntimeContext.getState(
      new ValueStateDescriptor("b-d-p", Types.BOOLEAN)
    )
    bulkData = getRuntimeContext.getState(
      new ValueStateDescriptor("b-d", outputTypeInformation)
    )
    changelogVersion = getRuntimeContext.getState(
      new ValueStateDescriptor[Versioned]("c-l-v", versionTypeInformation)
    )
    registeredTime = getRuntimeContext.getState(
      new ValueStateDescriptor[JLong]("r-t", Types.LONG)
    )

    copyRowProjection = ProjectionCodeGenerator
      .generateProjection(
        CodeGeneratorContext.apply(new TableConfig),
        "CopyRowProjection",
        changelogInputRowType,
        outputRowType,
        (0 until outputRowType.getFieldCount).toArray
      )
      .newInstance(Thread.currentThread.getContextClassLoader)
      .asInstanceOf[Projection[RowData, RowData]]
  }

  override def close(): Unit = {}

  override def processElement1(
      in1: RowData,
      context: KeyedCoProcessFunction[RowData, RowData, RowData, RowData]#Context,
      collector: Collector[RowData]
  ): Unit = {
    if (getLastChangelogVersion() == null && !hasProcessedBulkData() && getRegisteredTime() == null) {
      val currProcessingTime = context.timerService().currentProcessingTime()
      val triggerTime = currProcessingTime + fixedDelay
      context.timerService().registerEventTimeTimer(triggerTime)
      bulkData.update(in1)
      registeredTime.update(triggerTime)
    }
  }

  override def onTimer(
      timestamp: Long,
      ctx: KeyedCoProcessFunction[RowData, RowData, RowData, RowData]#OnTimerContext,
      out: Collector[RowData]
  ): Unit = {
    if (getLastChangelogVersion() == null) {
      out.collect(getBulkData())
      bulkDataProcessed.update(true)
    }
    bulkData.clear()
    registeredTime.clear()

  }

  override def processElement2(
      in2: RowData,
      context: KeyedCoProcessFunction[RowData, RowData, RowData, RowData]#Context,
      collector: Collector[RowData]
  ): Unit = {

    val lastChangelogVersion = getLastChangelogVersion()
    val currChangelogVersion = getChangelogVersionRowDataFromRowData(in2)
    lazy val projectedRow = projectRowData(in2)
    val bulkDataHasNotProcessed = !hasProcessedBulkData()
    val lastVersionIsNull = lastChangelogVersion == null

    def collectRow = collector.collect(projectedRow)

    def clearBulkDataProcessed =
      bulkDataProcessed.clear() // cuz no more used anymore

    def updateChangelogVersion = changelogVersion.update(currChangelogVersion)

    (lastVersionIsNull, bulkDataHasNotProcessed) match {
      case (true, true) =>
        in2.getRowKind match {
          case RowKind.INSERT | RowKind.UPDATE_AFTER =>
            projectedRow.setRowKind(RowKind.INSERT)
            collectRow
          case _ =>
        }
        updateChangelogVersion
      case (true, false) =>
        assert(
          in2.getRowKind != RowKind.INSERT && in2.getRowKind != RowKind.DELETE
        )
        collectRow
        updateChangelogVersion
        clearBulkDataProcessed
      case (false, _) =>
        if (currChangelogVersion.compareTo(lastChangelogVersion) > 0) {
          collectRow
          updateChangelogVersion
        } else {
          logger.info(
            s"get older version:$currChangelogVersion, last version is:$lastChangelogVersion"
          )
        }
    }

  }

  private[integrate] def projectRowData(in: RowData): RowData = {
    val re = in match {
      case joinedRowData: JoinedRowData =>
        RowDataUtils.getRow1FromJoinedRowData(joinedRowData)
      case _ =>
        copyRowProjection.apply(in)
    }
    re.setRowKind(in.getRowKind)
    re
  }

  private[integrate] def getChangelogVersionRowDataFromRowData(
      in: RowData
  ): Versioned = {

    in.getRawValue[Versioned](in.getArity - 1).toObject(versionTypeSer)
  }

  private[integrate] def getLastChangelogVersion(
      versionState: ValueState[Versioned] = this.changelogVersion
  ): Versioned = {
    versionState.value()
  }

  private[integrate] def hasProcessedBulkData(
      bulkDataProcessed: ValueState[JBoolean] = this.bulkDataProcessed
  ): Boolean = {
    Option(bulkDataProcessed.value()).map(_.booleanValue()).getOrElse(false)
  }

  private[integrate] def getRegisteredTime(
      registeredTime: ValueState[JLong] = this.registeredTime
  ): JLong = {
    registeredTime.value()
  }

  private[integrate] def getBulkData(
      bulkData: ValueState[RowData] = this.bulkData
  ): RowData = {
    bulkData.value()
  }

}
