package com.neighborhood.aka.laplace.hackathon.util;

import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.utils.JoinedRowData;

import java.lang.reflect.Field;

public class RowDataUtils {

    private static Field ROW1_FIELD;
    private static Field ROW2_FIELD;

    static {
        try {
            ROW1_FIELD = JoinedRowData.class.getField("row1");
            ROW1_FIELD.setAccessible(true);
            ROW2_FIELD = JoinedRowData.class.getField("row2");
            ROW2_FIELD.setAccessible(true);
        } catch (NoSuchFieldException e) {
            throw new RuntimeException(e);
        }
    }

    public static RowData getRow1FromJoinedRowData(JoinedRowData rowData) {
        try {
            return (RowData) ROW1_FIELD.get(rowData);
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    public static RowData getRow2FromJoinedRowData(JoinedRowData rowData) {
        try {
            return (RowData) ROW2_FIELD.get(rowData);
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }
}
