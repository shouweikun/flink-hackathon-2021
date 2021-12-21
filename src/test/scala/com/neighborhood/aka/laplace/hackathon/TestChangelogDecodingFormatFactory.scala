/* (C)2021 */
package com.neighborhood.aka.laplace.hackathon

import com.neighborhood.aka.laplace.hackathon.version.Versioned
import org.apache.flink.api.common.ExecutionConfig
import org.apache.flink.api.common.serialization.DeserializationSchema
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.configuration.{ConfigOption, ReadableConfig}
import org.apache.flink.table.connector.ChangelogMode
import org.apache.flink.table.connector.format.DecodingFormat
import org.apache.flink.table.connector.source.DynamicTableSource
import org.apache.flink.table.data.RowData
import org.apache.flink.table.factories.{
  DecodingFormatFactory,
  DeserializationFormatFactory,
  DynamicTableFactory
}
import org.apache.flink.table.types.DataType
import org.apache.flink.table.types.logical.RowType
import org.apache.flink.types.RowKind

import java.util

class TestChangelogDecodingFormatFactory extends DeserializationFormatFactory {

  override def createDecodingFormat(
      context: DynamicTableFactory.Context,
      readableConfig: ReadableConfig
  ): DecodingFormat[DeserializationSchema[RowData]] = {
    new DecodingFormat[DeserializationSchema[RowData]] {
      override def createRuntimeDecoder(
          context: DynamicTableSource.Context,
          dataType: DataType
      ): DeserializationSchema[RowData] = {
        new TestChangelogDeserializationSchema(
          dataType.getLogicalType.asInstanceOf[RowType]
        )
      }

      override def getChangelogMode: ChangelogMode =
        ChangelogMode
          .newBuilder()
          .addContainedKind(RowKind.DELETE)
          .addContainedKind(RowKind.INSERT)
          .addContainedKind(RowKind.UPDATE_AFTER)
          .addContainedKind(RowKind.UPDATE_BEFORE)
          .build()
    }
  }

  override def factoryIdentifier(): String = "TEST-CHANGELOG"

  override def requiredOptions() = new java.util.HashSet[ConfigOption[_]]()
  override def optionalOptions() = new java.util.HashSet[ConfigOption[_]]()
}
