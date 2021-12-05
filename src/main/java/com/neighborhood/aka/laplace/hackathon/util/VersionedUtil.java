package com.neighborhood.aka.laplace.hackathon.util;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RawType;
import org.apache.flink.table.types.logical.RowType;

import com.neighborhood.aka.laplace.hackathon.version.Versioned;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class VersionedUtil {

    public static RowType buildRowTypeWithVersioned(
            RowType rowType, TypeSerializer<Versioned> ser) {
        List<LogicalType> types = new ArrayList();
        types.addAll(
                rowType.getFields().stream()
                        .map(RowType.RowField::getType)
                        .collect(Collectors.toList()));
        types.add(new RawType(Versioned.class, ser));
        return RowType.of(types.toArray(new LogicalType[0]));
    }
}
