package com.neighborhood.aka.laplace.hackathon.version;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public interface VersionedDeserializationSchema<V extends Versioned<V>> extends DeserializationSchema<RowData> {


    RowType getRowDataType();

    TypeSerializer<V> getVersionTypeSerializer();

    Class<V> getVersionClass();


    @Override
    default RowData deserialize(byte[] bytes) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    default TypeInformation<RowData> getProducedType() {
        List<LogicalType> types = new ArrayList();
        types.addAll(getRowDataType().getFields().stream().map(RowType.RowField::getType).collect(Collectors.toList()));
        types.add(VersionUtils.createVersionType(getVersionClass(), getVersionTypeSerializer()));
        return InternalTypeInfo.of(RowType.of(types.toArray(new LogicalType[0])));
    }
}
