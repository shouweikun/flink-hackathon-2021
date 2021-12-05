package com.neighborhood.aka.laplace.hackathon;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.RowType;

import com.neighborhood.aka.laplace.hackathon.util.VersionedUtil;
import com.neighborhood.aka.laplace.hackathon.version.Versioned;

import java.io.IOException;

public interface VersionedDeserializationSchema extends DeserializationSchema<RowData> {

    RowType getRowDataType();

    default TypeSerializer<Versioned> getVersionTypeSerializer() {
        return TypeInformation.of(Versioned.class).createSerializer(new ExecutionConfig());
    }

    @Override
    default RowData deserialize(byte[] bytes) throws IOException {
        throw new UnsupportedOperationException();
    }

    default RowType getActualRowType() {
        return VersionedUtil.buildRowTypeWithVersioned(
                getRowDataType(), getVersionTypeSerializer());
    }

    @Override
    default TypeInformation<RowData> getProducedType() {

        return InternalTypeInfo.of(getActualRowType());
    }
}
