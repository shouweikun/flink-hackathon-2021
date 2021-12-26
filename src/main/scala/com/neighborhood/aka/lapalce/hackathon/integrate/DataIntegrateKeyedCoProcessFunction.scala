/* (C)2021 */
package com.neighborhood.aka.lapalce.hackathon.integrate

import com.neighborhood.aka.laplace.hackathon.util.RowDataUtils
import com.neighborhood.aka.laplace.hackathon.version.Versioned
import org.apache.flink.api.common.state.{
  MapState,
  MapStateDescriptor,
  ValueState,
  ValueStateDescriptor
}
import org.apache.flink.api.common.typeinfo.{TypeInformation, Types}
import org.apache.flink.api.common.typeutils.TypeSerializer
import org.apache.flink.configuration.Configuration
import org.apache.flink.shaded.guava18.com.google.common.collect.Lists
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction
import org.apache.flink.table.api.TableConfig
import org.apache.flink.table.data.RowData
import org.apache.flink.table.data.utils.JoinedRowData
import org.apache.flink.table.planner.codegen.{
  CodeGeneratorContext,
  ProjectionCodeGenerator
}
import org.apache.flink.table.runtime.generated.Projection
import org.apache.flink.table.runtime.typeutils.RowDataSerializer
import org.apache.flink.table.types.logical.RowType
import org.apache.flink.types.RowKind
import org.apache.flink.util.Collector
import org.slf4j.LoggerFactory

import java.lang.{Boolean => JBoolean, Long => JLong}
import java.util.{List => JList}

class DataIntegrateKeyedCoProcessFunction(
    private val fixedDelay: Long,
    private val versionTypeSer: TypeSerializer[Versioned],
    private val changelogInputRowType: RowType,
    private val outputRowType: RowType,
    private val outputTypeInformation: TypeInformation[RowData],
    private val versionTypeInformation: TypeInformation[Versioned],
    private val sendDataBehindWatermark: Boolean = false,
    private val isDebug: Boolean = false
) extends KeyedCoProcessFunction[RowData, RowData, RowData, RowData] {

  private final val logger =
    LoggerFactory.getLogger(classOf[DataIntegrateKeyedCoProcessFunction])

  private var bulkDataProcessed: ValueState[JBoolean] = _
  private var bulkData: ValueState[RowData] = _
  private var changelogVersion: ValueState[Versioned] = _
  private var registeredTime: ValueState[JLong] = _
  private var registeredChangelogData: MapState[JLong, JList[RowData]] = _

  private var copyRowProjection: Projection[RowData, RowData] = _
  private var rowDataSerializer: RowDataSerializer = _

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

    registeredChangelogData = getRuntimeContext.getMapState(
      new MapStateDescriptor[JLong, JList[RowData]](
        "r-c-d",
        Types.LONG,
        Types.LIST(outputTypeInformation)
      )
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

    rowDataSerializer = new RowDataSerializer(outputRowType)
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
    def collectRow(rowData: RowData): Unit = {
      logger.info(
        s"collect row at:$timestamp, watermark:${ctx.timerService().currentWatermark()}"
      )
      out.collect(rowData)
    }
    //start part of changelog data
    val data = registeredChangelogData.get(timestamp)
    val hasChangelogData = data != null && !data.isEmpty
    if (hasChangelogData) {
      val iter = data.iterator()
      while (iter.hasNext) {
        val curr = iter.next()
        collectRow(curr)
      }
      registeredChangelogData.remove(timestamp)
    }
    // end part of changelog data

    // start part of bulk data
    if (registeredTime.value() != null) {
      if (!hasChangelogData && getLastChangelogVersion() == null) {
        collectRow(getBulkData())
        bulkDataProcessed.update(true)
      }
      bulkData.clear()
      registeredTime.clear()
    }
    //end part of bulk data
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

    // internal func start
    def collectRow = collector.collect(projectedRow)

    def cacheRowDataAndRegisterTimer = {
      val ts = currChangelogVersion.getGeneratedTs
      val rowCopy = rowDataSerializer.copy(projectedRow)
      if (registeredChangelogData.contains(ts)) {
        val list = registeredChangelogData.get(ts)
        list.add(rowCopy)
        registeredChangelogData.put(
          ts,
          list
        )
      } else {
        val list = Lists.newArrayList(rowCopy)
        registeredChangelogData.put(ts, list)
      }

      context.timerService().registerEventTimeTimer(ts)
    }

    def sendOrCacheRowData =
      if (sendDataBehindWatermark) {
        cacheRowDataAndRegisterTimer
      } else {
        collectRow
      }

    def clearBulkDataProcessed =
      bulkDataProcessed.clear() // cuz no more used anymore

    def updateChangelogVersion = changelogVersion.update(currChangelogVersion)
    //internal function end

    (lastVersionIsNull, bulkDataHasNotProcessed) match {
      case (true, true) =>
        //the first data of this key ever.
        in2.getRowKind match {
          case RowKind.INSERT | RowKind.UPDATE_AFTER =>
            projectedRow.setRowKind(RowKind.INSERT)
            sendOrCacheRowData
          case _ =>
        }
        updateChangelogVersion
      case (true, false) =>
        assert(
          in2.getRowKind != RowKind.INSERT && in2.getRowKind != RowKind.DELETE
        )
        sendOrCacheRowData
        updateChangelogVersion
        clearBulkDataProcessed
      case (false, _) =>
        if (currChangelogVersion.compareTo(lastChangelogVersion) > 0) {
          sendOrCacheRowData
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
