package com.neighborhood.aka.lapalce.hackathon.source

import com.neighborhood.aka.laplace.hackathon.version.VersionedDeserializationSchema
import org.apache.flink.shaded.guava18.com.google.common.base.Optional
import org.apache.flink.table.api.TableSchema
import org.apache.flink.table.connector.ChangelogMode
import org.apache.flink.table.connector.format.DecodingFormat
import org.apache.flink.table.connector.source.{DynamicTableSource, ScanTableSource}
import org.apache.flink.table.data.RowData
import org.apache.flink.table.factories.FactoryUtil

class UnifiedTableSource(
                          private val bulkSource: ScanTableSource,
                          private val realtimeChangelogSource: ScanTableSource,
                          private val tableSchema: TableSchema,
                          private val decodingFormat: DecodingFormat

                            [VersionedDeserializationSchema]
                        ) extends ScanTableSource {

  override def getChangelogMode: ChangelogMode = ???

  override def getScanRuntimeProvider(scanContext: ScanTableSource.ScanContext): ScanTableSource.ScanRuntimeProvider = {

    val dataType = tableSchema.toPhysicalRowDataType;

    val versionedDeserializationSchema = decodingFormat.createRuntimeDecoder(scanContext, dataType)
    val versionClass = versionedDeserializationSchema.getVersionClass
    val versionTypeSer = versionedDeserializationSchema.getVersionTypeSerializer
    Optional

  }

  override def copy(): DynamicTableSource = ???

  override def asSummaryString(): String = ???
}
