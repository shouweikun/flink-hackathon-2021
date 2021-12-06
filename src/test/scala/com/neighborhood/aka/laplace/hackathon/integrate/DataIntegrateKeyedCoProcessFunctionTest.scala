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
import org.apache.flink.table.api.{DataTypes, TableConfig, TableSchema}
import org.apache.flink.table.data.{GenericRowData, RawValueData, RowData}
import org.apache.flink.table.planner.codegen.{
  CodeGeneratorContext,
  EqualiserCodeGenerator,
  ProjectionCodeGenerator
}
import org.apache.flink.table.planner.plan.utils.KeySelectorUtil
import org.apache.flink.table.runtime.generated.{Projection, RecordEqualiser}
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo
import org.apache.flink.table.types.AtomicDataType
import org.apache.flink.table.types.logical.{IntType, RowType}
import org.apache.flink.table.utils.TableSchemaUtils
import org.apache.flink.types.RowKind
import org.junit.Assert.{assertEquals, assertNull, assertTrue}
import org.junit.{After, Before, Test}

import scala.collection.JavaConversions._

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

  val fixedDelay = 100

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

  private var changelogRecordEqualiser: RecordEqualiser = _

  private var copyRowProjection: Projection[RowData, RowData] = _

  @Before
  def setup(): Unit = {
    harnessWithoutWatermarkAlign = createHarness(false)
    harnessWithWatermarkAlign = createHarness(true)

    harnessWithWatermarkAlign.open()
    harnessWithoutWatermarkAlign.open()

    changelogRecordEqualiser =
      new EqualiserCodeGenerator(changelogRowType.getChildren.toList.toArray)
        .generateRecordEqualiser("DeduplicateRowEqualiser")
        .newInstance(Thread.currentThread().getContextClassLoader)

    copyRowProjection = ProjectionCodeGenerator
      .generateProjection(
        CodeGeneratorContext.apply(new TableConfig),
        "CopyRowProjection",
        changelogRowType,
        outputRowType,
        (0 until outputRowType.getFieldCount).toArray
      )
      .newInstance(Thread.currentThread.getContextClassLoader)
      .asInstanceOf[Projection[RowData, RowData]]
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
      fixedDelay = fixedDelay,
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

  private def createBulkStreamRecord(k: Int, v: Int) = {
    val rowData = new GenericRowData(2)
    rowData.setField(0, k)
    rowData.setField(1, v)
    rowData.setRowKind(RowKind.INSERT)
    new StreamRecord[RowData](rowData)
  }

  private def createChangelogStreamRecord(
      k: Int,
      v: Int,
      rowKind: RowKind,
      version: Versioned
  ) = {
    val rowData = new GenericRowData(3)
    rowData.setField(0, k)
    rowData.setField(1, v)
    rowData.setField(2, RawValueData.fromObject(version))
    rowData.setRowKind(RowKind.INSERT)
    new StreamRecord[RowData](rowData)
  }

  @Test
  def testBulkDataSendWhenWatermarkBeyondTimestamp(): Unit = {

    val record = createBulkStreamRecord(1, 1)
    harnessWithoutWatermarkAlign.processElement1(record)
    harnessWithWatermarkAlign.processElement1(record)
    assertTrue(harnessWithWatermarkAlign.getOutput.isEmpty)
    assertTrue(harnessWithoutWatermarkAlign.getOutput.isEmpty)
    harnessWithoutWatermarkAlign.processWatermark2(
      new Watermark(fixedDelay + 1)
    )
    harnessWithWatermarkAlign.processWatermark2(new Watermark(fixedDelay + 1))
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

  @Test
  def testBulkDataSendWhenWatermarkEvictedByChangelogData(): Unit = {

    val bulkData = createBulkStreamRecord(1, 1)
    val changelogData1 = createChangelogStreamRecord(
      1,
      1,
      RowKind.INSERT,
      Versioned.of(fixedDelay - 1, fixedDelay - 1, false)
    )
    val changelogData2 = createChangelogStreamRecord(
      1,
      1,
      RowKind.DELETE,
      Versioned.of(fixedDelay + 1, fixedDelay + 1, false)
    )

    //non-aligned
    harnessWithoutWatermarkAlign.processElement1(bulkData)
    harnessWithoutWatermarkAlign.processElement2(changelogData1)
    assertTrue(
      changelogRecordEqualiser.equals(
        copyRowProjection.apply(changelogData1.getValue),
        harnessWithoutWatermarkAlign.getOutput
          .poll()
          .asInstanceOf[StreamRecord[RowData]]
          .getValue
      )
    )
    harnessWithoutWatermarkAlign.processElement2(changelogData2)
    assertTrue(
      changelogRecordEqualiser.equals(
        copyRowProjection.apply(changelogData2.getValue),
        harnessWithoutWatermarkAlign.getOutput
          .poll()
          .asInstanceOf[StreamRecord[RowData]]
          .getValue
      )
    )

    harnessWithoutWatermarkAlign.processWatermark2(
      new Watermark(fixedDelay + 1)
    )
    assertEquals(
      new Watermark(fixedDelay + 1),
      harnessWithoutWatermarkAlign.getOutput.poll()
    )
    assertNull(harnessWithoutWatermarkAlign.getOutput.poll())
  }
}
