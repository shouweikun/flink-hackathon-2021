package com.neighborhood.aka.laplace.hackathon

import org.apache.flink.api.common.serialization.DeserializationSchema
import org.apache.flink.configuration.ConfigOption
import org.apache.flink.table.api.TableSchema
import org.apache.flink.table.connector.ChangelogMode
import org.apache.flink.table.connector.format.DecodingFormat
import org.apache.flink.table.connector.source.{DynamicTableSource, ScanTableSource, SourceFunctionProvider}
import org.apache.flink.table.data.RowData
import org.apache.flink.table.factories.{DeserializationSchemaFactory, DynamicTableFactory, DynamicTableSourceFactory, FactoryUtil}

import scala.collection.JavaConversions._
import java.util

class DummyValueTableFactory extends DynamicTableSourceFactory {


  class DummyDynamicTableSource(
                                 val format: DecodingFormat[DeserializationSchema[RowData]],
                                 val tableSchema: TableSchema
                               ) extends ScanTableSource {

    override def getChangelogMode: ChangelogMode = ChangelogMode
      .newBuilder()
      .build()

    override def getScanRuntimeProvider(scanContext: ScanTableSource.ScanContext): ScanTableSource.ScanRuntimeProvider = {
      SourceFunctionProvider
        .of(
          new DummySourceFunction(format.createRuntimeDecoder(scanContext, tableSchema.toPhysicalRowDataType)), false
        )
    }

    override def copy(): DynamicTableSource = new DummyDynamicTableSource(format, tableSchema)

    override def asSummaryString(): String = "dummy"
  }

  override def factoryIdentifier(): String = "dummy"

  override def requiredOptions(): util.Set[ConfigOption[_]] = Set[ConfigOption[_]]()

  override def optionalOptions(): util.Set[ConfigOption[_]] = Set[ConfigOption[_]]()


  override def createDynamicTableSource(context: DynamicTableFactory.Context): DynamicTableSource = {
    val helper = FactoryUtil.createTableFactoryHelper(this, context)
    val decodingFormat = helper.discoverDecodingFormat[DeserializationSchema[RowData], DeserializationSchemaFactory[RowData]](classOf[DeserializationSchemaFactory[RowData]], FactoryUtil.FORMAT)
    new DummyDynamicTableSource(decodingFormat, context.getCatalogTable.getSchema)
  }
}
