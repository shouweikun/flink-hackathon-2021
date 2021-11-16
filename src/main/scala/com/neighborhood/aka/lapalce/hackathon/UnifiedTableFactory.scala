package com.neighborhood.aka.lapalce.hackathon

import com.neighborhood.aka.lapalce.hackathon.UnifiedTableFactory.{FIXED_DELAY, InternalContext, getBulkOptions, getRealtimeChangeOptions}
import com.neighborhood.aka.lapalce.hackathon.source.UnifiedTableSource
import com.neighborhood.aka.laplace.hackathon.VersionedDeserializationSchema
import org.apache.flink.api.common.serialization.DeserializationSchema
import org.apache.flink.configuration.{ConfigOption, ConfigOptions, ReadableConfig}
import org.apache.flink.table.catalog.{CatalogTable, ObjectIdentifier}
import org.apache.flink.table.connector.format.DecodingFormat
import org.apache.flink.table.connector.source.{DynamicTableSource, ScanTableSource}
import org.apache.flink.table.data.RowData
import org.apache.flink.table.factories.{DeserializationFormatFactory, DynamicTableFactory, DynamicTableSourceFactory, FactoryUtil}

import java.lang.{Long => JLong}
import java.util
import scala.collection.JavaConversions._

class UnifiedTableFactory extends DynamicTableSourceFactory {

  override def createDynamicTableSource(context: DynamicTableFactory.Context): DynamicTableSource = {

    val catalogTable = context.getCatalogTable
    val options = catalogTable.getOptions
    val bulkOptions = getBulkOptions(options)
    val changelogOptions = getRealtimeChangeOptions(options)

    val bulkTableSource = {
      val bulkTable = catalogTable.copy(bulkOptions)
      FactoryUtil.createTableSource(
        null,
        context.getObjectIdentifier,
        bulkTable,
        context.getConfiguration,
        context.getClassLoader,
        context.isTemporary
      )
    }
    val (realtimeChangelogSource, changelogFormat) = {
      val changelogTable = catalogTable.copy(changelogOptions)
      val source = FactoryUtil.createTableSource(
        null,
        context.getObjectIdentifier,
        changelogTable,
        context.getConfiguration,
        context.getClassLoader,
        context.isTemporary
      )
      val helper = FactoryUtil.createTableFactoryHelper(this, new InternalContext(changelogTable, context))
      val format = helper
        .discoverDecodingFormat[DeserializationSchema[RowData], DeserializationFormatFactory](classOf[DeserializationFormatFactory], FactoryUtil.FORMAT)

      (source, format)
    }

    assert(bulkTableSource.isInstanceOf[ScanTableSource])
    assert(realtimeChangelogSource.isInstanceOf[ScanTableSource])

    val helper = FactoryUtil.createTableFactoryHelper(this, context)
    val fixedDelay = helper.getOptions.get(FIXED_DELAY).longValue()

    new UnifiedTableSource(
      bulkTableSource.asInstanceOf[ScanTableSource],
      realtimeChangelogSource.asInstanceOf[ScanTableSource],
      catalogTable.getSchema,
      changelogFormat.asInstanceOf[DecodingFormat[VersionedDeserializationSchema]],
      fixedDelay

    )
  }

  override def factoryIdentifier(): String = "unified"

  override def requiredOptions(): util.Set[ConfigOption[_]] = Set[ConfigOption[_]](

  )

  override def optionalOptions(): util.Set[ConfigOption[_]] = Set[ConfigOption[_]](
    FIXED_DELAY
  )
}

object UnifiedTableFactory {

  val BULK_PREFIX = "_bulk."
  val CHANGELOG_PREFIX = "_changelog."

  private class InternalContext(
                                 catalogTable: CatalogTable,
                                 outerContext: DynamicTableFactory.Context
                               ) extends DynamicTableFactory.Context {
    override def getObjectIdentifier: ObjectIdentifier = outerContext.getObjectIdentifier

    override def getCatalogTable: CatalogTable = catalogTable

    override def getConfiguration: ReadableConfig = outerContext.getConfiguration

    override def getClassLoader: ClassLoader = outerContext.getClassLoader

    override def isTemporary: Boolean = outerContext.isTemporary
  }

  def getBulkOptions(options: java.util.Map[String, String]): java.util.Map[String, String] = {
    getOptions(BULK_PREFIX, options)
  }

  def getRealtimeChangeOptions(options: java.util.Map[String, String]): java.util.Map[String, String] = {
    getOptions(CHANGELOG_PREFIX, options)
  }

  def getOptions(prefix: String, options: java.util.Map[String, String]): java.util.Map[String, String] = {
    options
      .filter { case (k, _) => k.startsWith(prefix) }
      .map { case (k, v) => k.substring(prefix.length) -> v }
  }

  val FIXED_DELAY: ConfigOption[JLong] = ConfigOptions
    .key("fixed-delay")
    .longType()
    .defaultValue(10 * 1000)
    .withDescription("")

}
