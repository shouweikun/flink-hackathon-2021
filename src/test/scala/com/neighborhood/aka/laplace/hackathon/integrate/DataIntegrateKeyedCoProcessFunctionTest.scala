/* (C)2021 */
package com.neighborhood.aka.laplace.hackathon.integrate

import com.neighborhood.aka.lapalce.hackathon.integrate.{
  DataIntegrateKeyedCoProcessFunction,
  SpecializedKeyedCoProcessOperator
}
import com.neighborhood.aka.laplace.hackathon.version.Versioned
import org.apache.flink.api.common.ExecutionConfig
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.common.typeutils.TypeSerializer
import DataIntegrateKeyedCoProcessFunctionTest._
import com.neighborhood.aka.laplace.hackathon.util.VersionedUtil
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord
import org.apache.flink.streaming.util.KeyedTwoInputStreamOperatorTestHarness
import org.apache.flink.table.api.{DataTypes, TableSchema}
import org.apache.flink.table.data.{GenericRowData, RowData}
import org.apache.flink.table.planner.plan.utils.KeySelectorUtil
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo
import org.apache.flink.table.types.AtomicDataType
import org.apache.flink.table.types.logical.{IntType, RowType}
import org.apache.flink.table.utils.TableSchemaUtils
import org.apache.flink.types.RowKind
import org.junit.Assert.{assertEquals, assertTrue}
import org.junit.{After, Before, Test}

object DataIntegrateKeyedCoProcessFunctionTest {

  val versionTypeInformation = TypeInformation.of(classOf[Versioned])
  val versionTypeSer: TypeSerializer[Versioned] =
    versionTypeInformation.createSerializer(new ExecutionConfig)
  val tableSchema = TableSchema
    .builder()
    .field("k", new AtomicDataType(new IntType(false)))
    .field("v", DataTypes.INT())
    .primaryKey("k")
    .build()

  val outputRowType =
    tableSchema.toPhysicalRowDataType.getLogicalType.asInstanceOf[RowType]
  val changelogRowType =
    VersionedUtil.buildRowTypeWithVersioned(outputRowType, versionTypeSer)

}

class DataIntegrateKeyedCoProcessFunctionTest {

  private var harnessWithWatermarkAlign: KeyedTwoInputStreamOperatorTestHarness[
    RowData,
    RowData,
    RowData,
    RowData
  ] = _

  private var harnessWithoutWatermarkAlign
      : KeyedTwoInputStreamOperatorTestHarness[
        RowData,
        RowData,
        RowData,
        RowData
      ] = _

  @Before
  def setup(): Unit = {
    harnessWithoutWatermarkAlign = createHarness(false)
    harnessWithWatermarkAlign = createHarness(true)

    harnessWithWatermarkAlign.open()
    harnessWithoutWatermarkAlign.open()
  }

  @After
  def close(): Unit = {
    Option(harnessWithoutWatermarkAlign).foreach(_.close())
    Option(harnessWithWatermarkAlign).foreach(_.close)
    harnessWithWatermarkAlign = null
    harnessWithoutWatermarkAlign = null
  }

  private def createHarness(sendDataBehindWatermark: Boolean) = {
    val function = new DataIntegrateKeyedCoProcessFunction(
      fixedDelay = 10,
      versionTypeSer = versionTypeSer,
      changelogInputRowType = changelogRowType,
      outputRowType = outputRowType,
      outputTypeInformation = InternalTypeInfo.of(outputRowType),
      versionTypeInformation = versionTypeInformation,
      sendDataBehindWatermark
    )

    val primaryKey = TableSchemaUtils.getPrimaryKeyIndices(tableSchema)

    val bulkKeySelector = KeySelectorUtil.getRowDataSelector(
      primaryKey,
      InternalTypeInfo.of(outputRowType)
    )
    val changelogKeySelector = KeySelectorUtil.getRowDataSelector(
      primaryKey,
      InternalTypeInfo.of(changelogRowType)
    )
    val operator = new SpecializedKeyedCoProcessOperator(function)
    new KeyedTwoInputStreamOperatorTestHarness(
      operator,
      bulkKeySelector,
      changelogKeySelector,
      bulkKeySelector.getProducedType
    )
  }

  private def createStreamRecord(k: Int, v: Int, rowKind: RowKind) = {
    val rowData = new GenericRowData(2)
    rowData.setField(0, k)
    rowData.setField(1, v)
    rowData.setRowKind(rowKind)
    new StreamRecord[RowData](rowData)
  }

  @Test
  def testBulkDataSendWhenWatermarkBeyondTimestamp(): Unit = {

    val record = createStreamRecord(1, 1, RowKind.INSERT)
    harnessWithoutWatermarkAlign.processElement1(record)
    harnessWithWatermarkAlign.processElement1(record)
    assertTrue(harnessWithWatermarkAlign.getOutput.isEmpty)
    assertTrue(harnessWithoutWatermarkAlign.getOutput.isEmpty)
    harnessWithoutWatermarkAlign.processWatermark2(new Watermark(100L))
    harnessWithWatermarkAlign.processWatermark2(new Watermark(100L))
    assertEquals(
      record.getValue,
      harnessWithWatermarkAlign.getOutput
        .poll()
        .asInstanceOf[StreamRecord[RowData]]
        .getValue
    )
    assertEquals(
      record.getValue,
      harnessWithoutWatermarkAlign.getOutput
        .poll()
        .asInstanceOf[StreamRecord[RowData]]
        .getValue
    )
  }

}
