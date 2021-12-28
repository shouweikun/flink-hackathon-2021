package com.neighborhood.aka.laplace.hackathon.formats.estuary;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.formats.json.JsonToRowDataConverters;
import org.apache.flink.formats.json.TimestampFormat;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.utils.LogicalTypeChecks;

import org.apache.flink.shaded.guava18.com.google.common.cache.Cache;
import org.apache.flink.shaded.guava18.com.google.common.cache.CacheBuilder;
import org.apache.flink.shaded.guava18.com.google.common.collect.ImmutableList;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.json.JsonReadFeature;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.DeserializationFeature;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import com.neighborhood.aka.laplace.hackathon.AbstractVersionedDeserializationSchema;
import com.neighborhood.aka.laplace.hackathon.version.Versioned;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.*;
import java.util.function.Function;

import static java.lang.String.format;
import static org.apache.flink.util.Preconditions.checkNotNull;

public class EstuaryTestDeserializationSchema extends AbstractVersionedDeserializationSchema {

    private static final long serialVersionUID = 1L;

    private static final String SQLSERVER_TS_FIELD_NAME = "cdcTimeStamp";

    private static final String MYSQL_TS_FIELD_NAME = "ts";

    private static final String TIDB_TS_FIELD_NAME = "searchTs";

    private static final String BEFORE = "before";

    private static final String AFTER = "after";

    /** Flag indicating whether to fail if a field is missing. */
    private final boolean failOnMissingField;

    /** Flag indicating whether to ignore invalid fields/rows (default: throw an exception). */
    private final boolean ignoreParseErrors;

    /**
     * Runtime converter that converts {@link JsonNode}s into objects of Flink SQL internal data
     * structures.
     */
    private final JsonToRowDataConverters.JsonToRowDataConverter runtimeConverter;

    /** Object mapper for parsing the JSON. */
    private final ObjectMapper objectMapper = new ObjectMapper();

    /** Timestamp format specification which is used to parse timestamp. */
    private final TimestampFormat timestampFormat;

    private final String dbType;

    private transient String tsFieldName;

    private transient Function<JsonNode, long[]> versionExtractFunction;

    private transient RowData dummyRow;

    public EstuaryTestDeserializationSchema(
            RowType rowType,
            boolean failOnMissingField,
            boolean ignoreParseErrors,
            TimestampFormat timestampFormat,
            String dbType) {
        super(rowType);
        if (ignoreParseErrors && failOnMissingField) {
            throw new IllegalArgumentException(
                    "JSON format doesn't support failOnMissingField and ignoreParseErrors are both enabled.");
        }
        this.failOnMissingField = failOnMissingField;
        this.ignoreParseErrors = ignoreParseErrors;
        this.runtimeConverter =
                new JsonToRowDataConverters(failOnMissingField, ignoreParseErrors, timestampFormat)
                        .createConverter(checkNotNull(rowType));
        this.timestampFormat = timestampFormat;
        boolean hasDecimalType =
                LogicalTypeChecks.hasNested(rowType, t -> t instanceof DecimalType);
        if (hasDecimalType) {
            objectMapper.enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS);
        }
        objectMapper.configure(JsonReadFeature.ALLOW_UNESCAPED_CONTROL_CHARS.mappedFeature(), true);
        this.dbType = dbType;
    }

    @Override
    public void open(InitializationContext context) throws Exception {
        this.dummyRow = new GenericRowData(getRowDataType().getFieldCount());

        if (dbType.equals("mysql")) {
            tsFieldName = MYSQL_TS_FIELD_NAME;
        } else if (dbType.equals("sqlserver")) {
            tsFieldName = SQLSERVER_TS_FIELD_NAME;
        } else if (dbType.equals("tidb")) {
            tsFieldName = TIDB_TS_FIELD_NAME;
        } else {
            throw new IllegalArgumentException();
        }

        if (dbType.equals("mysql")) {
            versionExtractFunction =
                    jsonNode -> {
                        JsonNode binlog = jsonNode.get("binlogPosition");
                        long offset = binlog.get("offset").asLong();
                        long file = Long.valueOf(binlog.get("file").asText().split("\\.")[1]);
                        return new long[] {file, offset, 0};
                    };
        } else if (dbType.equals("sqlserver")) {

            final Cache<Long, Long> tsCounterCache =
                    CacheBuilder.newBuilder().maximumSize(5).build();
            final String tsFieldName = this.tsFieldName;

            versionExtractFunction =
                    jsonNode -> {
                        long ts = jsonNode.get(tsFieldName).asLong();
                        long currProcessTs = System.currentTimeMillis();
                        Long count = tsCounterCache.getIfPresent(currProcessTs);
                        if (count == null) {
                            count = 0L;
                        } else {
                            count++;
                        }
                        tsCounterCache.put(currProcessTs, count);
                        return new long[] {ts, currProcessTs, count, 0};
                    };
        } else {
            versionExtractFunction =
                    jsonNode -> {
                        final String tsFieldName = this.tsFieldName;
                        long ts = jsonNode.get(tsFieldName).asLong();
                        return new long[] {ts, 0};
                    };
        }
    }

    public Collection<Tuple2<RowData, Versioned>> deserializeInternal(@Nullable byte[] message)
            throws IOException {
        if (message == null) {
            return Collections.EMPTY_SET;
        }
        try {
            JsonNode rootJsonNode = deserializeToJsonNode(message);
            String eventType =
                    Optional.ofNullable(rootJsonNode.get("opt"))
                            .map(JsonNode::asText)
                            .map(String::trim)
                            .orElse(null);
            long ts = rootJsonNode.get(tsFieldName).asLong();
            long[] version = versionExtractFunction.apply(rootJsonNode);
            if (eventType.equals("i")) {
                return ImmutableList.of(
                        Tuple2.of(
                                convertToRowData(rootJsonNode.get(AFTER)),
                                Versioned.of(ts, version, false)));
            } else if (eventType.equals("d")) {
                return ImmutableList.of(
                        Tuple2.of(
                                convertToRowData(rootJsonNode.get(BEFORE)),
                                Versioned.of(ts, version, false)));
            } else if (eventType.equals("u")) {

                long[] afterVersion = Arrays.copyOf(version, version.length);
                int index = afterVersion.length - 1;
                afterVersion[index] = afterVersion[index] + 1;

                return ImmutableList.of(
                        Tuple2.of(
                                convertToRowData(rootJsonNode.get(BEFORE)),
                                Versioned.of(ts, version, false)),
                        Tuple2.of(
                                convertToRowData(rootJsonNode.get(AFTER)),
                                Versioned.of(ts, afterVersion, false)));
            } else {
                return ImmutableList.of(Tuple2.of(dummyRow, Versioned.of(ts, null, true)));
            }
        } catch (Throwable t) {
            if (ignoreParseErrors) {
                return null;
            }
            throw new IOException(
                    format("Failed to deserialize JSON '%s'.", new String(message)), t);
        }
    }

    public JsonNode deserializeToJsonNode(byte[] message) throws IOException {
        return objectMapper.readTree(message);
    }

    public RowData convertToRowData(JsonNode message) {
        return (RowData) runtimeConverter.convert(message);
    }

    @Override
    public boolean isEndOfStream(RowData nextElement) {
        return false;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        EstuaryTestDeserializationSchema that = (EstuaryTestDeserializationSchema) o;
        return failOnMissingField == that.failOnMissingField
                && ignoreParseErrors == that.ignoreParseErrors
                && timestampFormat.equals(that.timestampFormat);
    }

    @Override
    public int hashCode() {
        return Objects.hash(failOnMissingField, ignoreParseErrors, timestampFormat);
    }
}
