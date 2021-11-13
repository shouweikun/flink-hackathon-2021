package com.neighborhood.aka.laplace.hackathon.version;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.table.types.logical.RawType;
import org.apache.flink.table.types.logical.RowType;

public class VersionUtils {

    public static <V extends Versioned<V>> RowType createVersionType(Class<V> versionClass, TypeSerializer<V> typeSerializer) {
        return RowType.of(new RawType(versionClass, typeSerializer));
    }

}
