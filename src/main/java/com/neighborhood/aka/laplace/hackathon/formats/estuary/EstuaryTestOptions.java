package com.neighborhood.aka.laplace.hackathon.formats.estuary;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

public class EstuaryTestOptions {

    public static final ConfigOption<Boolean> FAIL_ON_MISSING_FIELD =
            ConfigOptions.key("fail-on-missing-field")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "Optional flag to specify whether to fail if a field is missing or not, false by default.");

    public static final ConfigOption<Boolean> IGNORE_PARSE_ERRORS =
            ConfigOptions.key("ignore-parse-errors")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "Optional flag to skip fields and rows with parse errors instead of failing;\n"
                                    + "fields are set to null in case of errors, false by default.");

    public static final ConfigOption<String> MAP_NULL_KEY_MODE =
            ConfigOptions.key("map-null-key.mode")
                    .stringType()
                    .defaultValue("FAIL")
                    .withDescription(
                            "Optional flag to control the handling mode when serializing null key for map data, FAIL by default."
                                    + " Option DROP will drop null key entries for map data."
                                    + " Option LITERAL will use 'map-null-key.literal' as key literal.");

    public static final ConfigOption<String> MAP_NULL_KEY_LITERAL =
            ConfigOptions.key("map-null-key.literal")
                    .stringType()
                    .defaultValue("null")
                    .withDescription(
                            "Optional flag to specify string literal for null keys when 'map-null-key.mode' is LITERAL, \"null\" by default.");

    public static final ConfigOption<String> TIMESTAMP_FORMAT =
            ConfigOptions.key("timestamp-format.standard")
                    .stringType()
                    .defaultValue("SQL")
                    .withDescription(
                            "Optional flag to specify timestamp format, SQL by default."
                                    + " Option ISO-8601 will parse input timestamp in \"yyyy-MM-ddTHH:mm:ss.s{precision}\" format and output timestamp in the same format."
                                    + " Option SQL will parse input timestamp in \"yyyy-MM-dd HH:mm:ss.s{precision}\" format and output timestamp in the same format.");

    public static final ConfigOption<Boolean> ENCODE_DECIMAL_AS_PLAIN_NUMBER =
            ConfigOptions.key("encode.decimal-as-plain-number")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "Optional flag to specify whether to encode all decimals as plain numbers instead of possible scientific notations, false by default.");

    public static final ConfigOption<Boolean> IGNORE_HEARTBEAT =
            ConfigOptions.key("ignore-heartbeat")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("");

    public static final ConfigOption<String> CONCERNED_TABLE_NAME =
            ConfigOptions.key("concerned-table-name").stringType().noDefaultValue();

    public static final ConfigOption<String> DB_TYPE =
            ConfigOptions.key("db-type").stringType().noDefaultValue().withDescription("");

    // --------------------------------------------------------------------------------------------
    // Enums
    // --------------------------------------------------------------------------------------------

    /** Handling mode for map data with null key. */
    public enum MapNullKeyMode {
        FAIL,
        DROP,
        LITERAL
    }

    private EstuaryTestOptions() {}
}
