package com.neighborhood.aka.laplace.hackathon.formats.estuary;

import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.formats.json.TimestampFormat;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.ValidationException;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import static com.neighborhood.aka.laplace.hackathon.formats.estuary.EstuaryTestOptions.*;

public class EstuaryTestOptionsUtil {

    // --------------------------------------------------------------------------------------------
    // Option enumerations
    // --------------------------------------------------------------------------------------------

    public static final String SQL = "SQL";
    public static final String ISO_8601 = "ISO-8601";

    public static final Set<String> TIMESTAMP_FORMAT_ENUM =
            new HashSet<>(Arrays.asList(SQL, ISO_8601));

    // The handling mode of null key for map data
    public static final String JSON_MAP_NULL_KEY_MODE_FAIL = "FAIL";
    public static final String JSON_MAP_NULL_KEY_MODE_DROP = "DROP";
    public static final String JSON_MAP_NULL_KEY_MODE_LITERAL = "LITERAL";

    // --------------------------------------------------------------------------------------------
    // Utilities
    // --------------------------------------------------------------------------------------------

    public static TimestampFormat getTimestampFormat(ReadableConfig config) {
        String timestampFormat = config.get(TIMESTAMP_FORMAT);
        switch (timestampFormat) {
            case SQL:
                return TimestampFormat.SQL;
            case ISO_8601:
                return TimestampFormat.ISO_8601;
            default:
                throw new TableException(
                        String.format(
                                "Unsupported timestamp format '%s'. Validator should have checked that.",
                                timestampFormat));
        }
    }

    /**
     * Creates handling mode for null key map data.
     *
     * <p>See {@link #JSON_MAP_NULL_KEY_MODE_FAIL}, {@link #JSON_MAP_NULL_KEY_MODE_DROP}, and {@link
     * #JSON_MAP_NULL_KEY_MODE_LITERAL} for more information.
     */
    public static EstuaryTestOptions.MapNullKeyMode getMapNullKeyMode(ReadableConfig config) {
        String mapNullKeyMode = config.get(MAP_NULL_KEY_MODE);
        switch (mapNullKeyMode.toUpperCase()) {
            case JSON_MAP_NULL_KEY_MODE_FAIL:
                return EstuaryTestOptions.MapNullKeyMode.FAIL;
            case JSON_MAP_NULL_KEY_MODE_DROP:
                return EstuaryTestOptions.MapNullKeyMode.DROP;
            case JSON_MAP_NULL_KEY_MODE_LITERAL:
                return EstuaryTestOptions.MapNullKeyMode.LITERAL;
            default:
                throw new TableException(
                        String.format(
                                "Unsupported map null key handling mode '%s'. Validator should have checked that.",
                                mapNullKeyMode));
        }
    }

    // --------------------------------------------------------------------------------------------
    // Validation
    // --------------------------------------------------------------------------------------------

    /** Validator for json decoding format. */
    public static void validateDecodingFormatOptions(ReadableConfig tableOptions) {
        boolean failOnMissingField = tableOptions.get(FAIL_ON_MISSING_FIELD);
        boolean ignoreParseErrors = tableOptions.get(IGNORE_PARSE_ERRORS);
        if (ignoreParseErrors && failOnMissingField) {
            throw new ValidationException(
                    FAIL_ON_MISSING_FIELD.key()
                            + " and "
                            + IGNORE_PARSE_ERRORS.key()
                            + " shouldn't both be true.");
        }
        validateTimestampFormat(tableOptions);
    }

    /** Validator for json encoding format. */
    public static void validateEncodingFormatOptions(ReadableConfig tableOptions) {
        // validator for {@link MAP_NULL_KEY_MODE}
        Set<String> nullKeyModes =
                Arrays.stream(EstuaryTestOptions.MapNullKeyMode.values())
                        .map(Objects::toString)
                        .collect(Collectors.toSet());
        if (!nullKeyModes.contains(tableOptions.get(MAP_NULL_KEY_MODE).toUpperCase())) {
            throw new ValidationException(
                    String.format(
                            "Unsupported value '%s' for option %s. Supported values are %s.",
                            tableOptions.get(MAP_NULL_KEY_MODE),
                            MAP_NULL_KEY_MODE.key(),
                            nullKeyModes));
        }
        validateTimestampFormat(tableOptions);
    }

    /** Validates timestamp format which value should be SQL or ISO-8601. */
    static void validateTimestampFormat(ReadableConfig tableOptions) {
        String timestampFormat = tableOptions.get(TIMESTAMP_FORMAT);
        if (!TIMESTAMP_FORMAT_ENUM.contains(timestampFormat)) {
            throw new ValidationException(
                    String.format(
                            "Unsupported value '%s' for %s. Supported values are [SQL, ISO-8601].",
                            timestampFormat, TIMESTAMP_FORMAT.key()));
        }
    }

    private EstuaryTestOptionsUtil() {}
}
