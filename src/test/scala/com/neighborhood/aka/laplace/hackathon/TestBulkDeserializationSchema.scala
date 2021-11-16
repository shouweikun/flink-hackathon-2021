package com.neighborhood.aka.laplace.hackathon

import org.apache.flink.api.common.serialization.DeserializationSchema
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.table.data.{GenericRowData, RowData}
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo
import org.apache.flink.table.types.logical.RowType
import org.apache.flink.util.Collector

class TestBulkDeserializationSchema(val rowType: RowType) extends DeserializationSchema[RowData] {

  override def deserialize(bytes: Array[Byte]): RowData = ???


  override def deserialize(message: Array[Byte], out: Collector[RowData]): Unit = {
    TestData.BULK_DATA.foreach {
      case (_, k, v) =>
        val data = new GenericRowData(2)
        data.setField(0, k)
        data.setField(1, v)
        out.collect(data)
    }
  }

  override def isEndOfStream(t: RowData): Boolean = false

  override def getProducedType: TypeInformation[RowData] = InternalTypeInfo.of(rowType)
}
