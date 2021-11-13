package com.neighborhood.aka.laplace.hackathon.version;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.utils.JoinedRowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.Collector;

import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;

public abstract class AbstractVersionedDeserializationSchema<V extends Versioned<V>> implements VersionedDeserializationSchema<V> {

    private final RowType rowType;
    private final TypeSerializer<V> versionTypeSerializer;
    private final Class<V> versionClass;

    public AbstractVersionedDeserializationSchema(
            RowType rowType,
            TypeSerializer<V> versionTypeSerializer,
            Class<V> versionClass
    ) {
        this.rowType = rowType;
        this.versionTypeSerializer = versionTypeSerializer;
        this.versionClass = versionClass;
    }


    @Override
    public RowType getRowDataType() {
        return this.rowType;
    }

    @Override
    public TypeSerializer<V> getVersionTypeSerializer() {
        return this.versionTypeSerializer;
    }

    @Override
    public Class<V> getVersionClass() {
        return this.versionClass;
    }

    @Override
    public void deserialize(byte[] message, Collector<RowData> out) throws IOException {
        Iterator<Tuple2<RowData, V>> iterator = deserializeInternal(message).iterator();
        while (iterator.hasNext()) {
            Tuple2<RowData, V> curr = iterator.next();
            out.collect(new JoinedRowData(curr.f0, GenericRowData.of(curr.f1)));
        }
    }

    protected abstract Collection<Tuple2<RowData, V>> deserializeInternal(byte[] bytes);
}
