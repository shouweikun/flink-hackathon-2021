package com.neighborhood.aka.laplace.hackathon;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RawValueData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.utils.JoinedRowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.Collector;

import com.neighborhood.aka.laplace.hackathon.version.Versioned;

import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;

public abstract class AbstractVersionedDeserializationSchema
        implements VersionedDeserializationSchema {

    private final RowType rowType;
    private final TypeSerializer<Versioned> versionTypeSerializer;

    public AbstractVersionedDeserializationSchema(
            RowType rowType, TypeSerializer<Versioned> versionTypeSerializer) {
        this.rowType = rowType;
        this.versionTypeSerializer = versionTypeSerializer;
    }

    @Override
    public RowType getRowDataType() {
        return this.rowType;
    }

    @Override
    public TypeSerializer<Versioned> getVersionTypeSerializer() {
        return this.versionTypeSerializer;
    }

    @Override
    public void deserialize(byte[] message, Collector<RowData> out) throws IOException {
        Iterator<Tuple2<RowData, Versioned>> iterator = deserializeInternal(message).iterator();
        while (iterator.hasNext()) {
            Tuple2<RowData, Versioned> curr = iterator.next();
            JoinedRowData row =
                    new JoinedRowData(curr.f0, GenericRowData.of(RawValueData.fromObject(curr.f1)));
            row.setRowKind(curr.f0.getRowKind());
            out.collect(row);
        }
    }

    protected abstract Collection<Tuple2<RowData, Versioned>> deserializeInternal(byte[] bytes);
}
