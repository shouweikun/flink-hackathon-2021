package com.neighborhood.aka.laplace.hackathon.formats.estuary;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.formats.common.TimestampFormat;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.DeserializationFormatFactory;
import org.apache.flink.table.factories.DynamicTableFactory;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;

import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

import static com.neighborhood.aka.laplace.hackathon.formats.estuary.EstuaryTestOptions.*;

public class EstuaryTestDeserializationFormatFactory implements DeserializationFormatFactory {

    private static final String IDENTIFIER = "ESTUARY_TEST";

    @Override
    public DecodingFormat<DeserializationSchema<RowData>> createDecodingFormat(
            DynamicTableFactory.Context context, ReadableConfig readableConfig) {
        return new DecodingFormat<DeserializationSchema<RowData>>() {
            @Override
            public DeserializationSchema<RowData> createRuntimeDecoder(
                    DynamicTableSource.Context context, DataType dataType) {
                final RowType logicalType = (RowType) dataType.getLogicalType();
                final TypeInformation<RowData> rowDataTypeInfo =
                        context.createTypeInformation(dataType);
                final boolean failOnMissingField = readableConfig.get(FAIL_ON_MISSING_FIELD);
                final boolean ignoreParseErrors = readableConfig.get(IGNORE_PARSE_ERRORS);
                final TimestampFormat timestampFormat =
                        EstuaryTestOptionsUtil.getTimestampFormat(readableConfig);
                final String dbType = readableConfig.get(DB_TYPE);
                final boolean ignoreHeartbeat = readableConfig.get(IGNORE_HEARTBEAT);
                final ObjectPath concernedTableName =
                        Optional.ofNullable(readableConfig.get(CONCERNED_TABLE_NAME))
                                .map(
                                        s -> {
                                            String[] split = s.split("\\.");
                                            return new ObjectPath(split[0], split[1]);
                                        })
                                .orElse(null);

                return new EstuaryTestDeserializationSchema(
                        logicalType,
                        failOnMissingField,
                        ignoreParseErrors,
                        timestampFormat,
                        dbType,
                        concernedTableName,
                        ignoreHeartbeat);
            }

            @Override
            public ChangelogMode getChangelogMode() {
                return ChangelogMode.newBuilder()
                        .addContainedKind(RowKind.DELETE)
                        .addContainedKind(RowKind.INSERT)
                        .addContainedKind(RowKind.UPDATE_BEFORE)
                        .addContainedKind(RowKind.UPDATE_AFTER)
                        .build();
            }
        };
    }

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        return new HashSet<>();
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        HashSet<ConfigOption<?>> options = new HashSet<>();
        options.add(FAIL_ON_MISSING_FIELD);
        options.add(IGNORE_PARSE_ERRORS);
        options.add(TIMESTAMP_FORMAT);
        options.add(MAP_NULL_KEY_MODE);
        options.add(MAP_NULL_KEY_LITERAL);
        options.add(ENCODE_DECIMAL_AS_PLAIN_NUMBER);
        options.add(DB_TYPE);
        options.add(IGNORE_HEARTBEAT);
        options.add(CONCERNED_TABLE_NAME);
        return options;
    }
}
