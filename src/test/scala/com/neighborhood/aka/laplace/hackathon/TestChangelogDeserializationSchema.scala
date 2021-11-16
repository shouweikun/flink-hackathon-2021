/* (C)2021 */
package com.neighborhood.aka.laplace.hackathon

import com.neighborhood.aka.laplace.hackathon.version.Versioned
import org.apache.flink.api.common.serialization.DeserializationSchema
import org.apache.flink.api.common.typeutils.TypeSerializer
import org.apache.flink.api.java.tuple
import org.apache.flink.table.data.{GenericRowData, RowData}
import org.apache.flink.table.types.logical.RowType
import org.apache.flink.types.RowKind

import java.util
import java.util.Collections
import scala.collection.JavaConversions._

class TestChangelogDeserializationSchema(
    val rowType: RowType,
    val versionTypeSerializer: TypeSerializer[Versioned]
) extends AbstractVersionedDeserializationSchema(
      rowType,
      versionTypeSerializer
    ) {

  private var data: List[(String, Int, Int, Long)] = _

  override def open(
      context: DeserializationSchema.InitializationContext
  ): Unit = {
    data = TestData.CHANGELOG_DATA
  }

  override protected def deserializeInternal(
      bytes: Array[Byte]
  ): util.Collection[tuple.Tuple2[RowData, Versioned]] = {
    data match {
      case List.empty =>
        Collections.emptyList()
      case head :: tail =>
        data = tail
        val (rowKindStr, k, v, ts) = head
        val row = new GenericRowData(2)
        row.setField(0, k)
        row.setField(1, v)
        val version = new Versioned()
        version.setGeneratedTs(ts)
        version.setUnifiedVersion(ts)
        rowKindStr.trim match {
          case "i"  => row.setRowKind(RowKind.INSERT)
          case "d"  => row.setRowKind(RowKind.DELETE)
          case "-u" => row.setRowKind(RowKind.UPDATE_BEFORE)
          case "+u" => row.setRowKind(RowKind.UPDATE_AFTER)
        }
        List(new tuple.Tuple2[RowData, Versioned](row, version))
    }
  }

  override def isEndOfStream(t: RowData): Boolean = false
}
