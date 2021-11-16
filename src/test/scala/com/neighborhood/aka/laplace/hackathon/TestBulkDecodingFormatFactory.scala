/* (C)2021 */
package com.neighborhood.aka.laplace.hackathon

import org.apache.flink.api.common.serialization.DeserializationSchema
import org.apache.flink.configuration.{ConfigOption, ReadableConfig}
import org.apache.flink.table.connector.ChangelogMode
import org.apache.flink.table.connector.format.DecodingFormat
import org.apache.flink.table.connector.source.DynamicTableSource
import org.apache.flink.table.data.RowData
import org.apache.flink.table.factories.{
  DecodingFormatFactory,
  DeserializationFormatFactory,
  DeserializationSchemaFactory,
  DynamicTableFactory
}
import org.apache.flink.table.types.DataType
import org.apache.flink.table.types.logical.RowType

class TestBulkDecodingFormatFactory extends DeserializationFormatFactory {

  override def createDecodingFormat(
      context: DynamicTableFactory.Context,
      readableConfig: ReadableConfig
  ): DecodingFormat[DeserializationSchema[RowData]] = {
    new DecodingFormat[DeserializationSchema[RowData]] {

      override def createRuntimeDecoder(
          context: DynamicTableSource.Context,
          dataType: DataType
      ): DeserializationSchema[RowData] = {
        new TestBulkDeserializationSchema(
          dataType.getLogicalType.asInstanceOf[RowType]
        )
      }

      override def getChangelogMode: ChangelogMode = ChangelogMode.insertOnly()
    }
  }

  override def factoryIdentifier(): String = "TEST-BULK"

  override def requiredOptions() = new java.util.HashSet[ConfigOption[_]]()

  override def optionalOptions() = new java.util.HashSet[ConfigOption[_]]()
}
