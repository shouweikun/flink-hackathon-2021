/* (C)2021 */
package com.neighborhood.aka.laplace.hackathon

import org.apache.flink.api.common.serialization.DeserializationSchema
import org.apache.flink.configuration.ConfigOption
import org.apache.flink.table.api.TableSchema
import org.apache.flink.table.connector.ChangelogMode
import org.apache.flink.table.connector.format.DecodingFormat
import org.apache.flink.table.connector.source.{
  DynamicTableSource,
  ScanTableSource,
  SourceFunctionProvider
}
import org.apache.flink.table.data.RowData
import org.apache.flink.table.factories.{
  DeserializationFormatFactory,
  DeserializationSchemaFactory,
  DynamicTableFactory,
  DynamicTableSourceFactory,
  FactoryUtil
}
import org.apache.flink.types.RowKind

import scala.collection.JavaConversions._
import java.util

class DummyValueTableFactory extends DynamicTableSourceFactory {

  class DummyDynamicTableSource(
      val format: DecodingFormat[DeserializationSchema[RowData]],
      val tableSchema: TableSchema
  ) extends ScanTableSource {

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
      SourceFunctionProvider
        .of(
          new DummySourceFunction(
            format.createRuntimeDecoder(
              scanContext,
              tableSchema.toPhysicalRowDataType
            )
          ),
          false
        )
    }

    override def copy(): DynamicTableSource =
      new DummyDynamicTableSource(format, tableSchema)

    override def asSummaryString(): String = "dummy"
  }

  override def factoryIdentifier(): String = "dummy"

  override def requiredOptions() = new java.util.HashSet[ConfigOption[_]]()

  override def optionalOptions() = new java.util.HashSet[ConfigOption[_]]()

  override def createDynamicTableSource(
      context: DynamicTableFactory.Context
  ): DynamicTableSource = {
    val helper = FactoryUtil.createTableFactoryHelper(this, context)
    val decodingFormat = helper
      .discoverDecodingFormat[DeserializationSchema[RowData], DeserializationFormatFactory](
        classOf[DeserializationFormatFactory],
        FactoryUtil.FORMAT
      )
    new DummyDynamicTableSource(
      decodingFormat,
      context.getCatalogTable.getSchema
    )
  }
}
