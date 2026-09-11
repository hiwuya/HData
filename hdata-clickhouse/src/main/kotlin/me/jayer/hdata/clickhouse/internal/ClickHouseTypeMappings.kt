package me.jayer.hdata.clickhouse.internal

import org.apache.beam.sdk.schemas.Schema

/**
 * Maps ClickHouse column types (from JDBC metadata) to Beam schema types.
 *
 * ClickHouse type names are case-insensitive and may carry parameters (e.g. `Decimal(18,4)`,
 * `LowCardinality(String)`, `Nullable(Int32)`). This mapper strips the parameters and matches the
 * base type.
 *
 * @author wuya
 */
internal object ClickHouseTypeMappings {

    fun toBeamType(clickhouseTypeName: String): Schema.FieldType {
        val normalized = clickhouseTypeName.trim().uppercase()

        // Strip Nullable wrapper.
        val base = if (normalized.startsWith("NULLABLE(")) {
            normalized.removePrefix("NULLABLE(").removeSuffix(")")
        } else {
            normalized
        }

        // Strip LowCardinality wrapper.
        val inner = if (base.startsWith("LOWCARDINALITY(")) {
            base.removePrefix("LOWCARDINALITY(").removeSuffix(")")
        } else {
            base
        }

        // Strip parameters: Decimal(18,4) -> DECIMAL, DateTime64(3) -> DATETIME64, etc.
        val typeName = inner.substringBefore('(').trim()

        return when (typeName) {
            // Strings
            "STRING", "VARCHAR", "CHAR", "FIXEDSTRING", "UUID", "JSON",
            "ENUM", "ENUM8", "ENUM16" -> Schema.FieldType.STRING

            // Integers
            "INT8", "TINYINT" -> Schema.FieldType.BYTE
            "INT16", "SMALLINT" -> Schema.FieldType.INT16
            "INT32", "INT", "INTEGER" -> Schema.FieldType.INT32
            "INT64", "BIGINT" -> Schema.FieldType.INT64
            "INT128" -> Schema.FieldType.DECIMAL
            "INT256" -> Schema.FieldType.DECIMAL
            "UINT8" -> Schema.FieldType.INT16  // unsigned byte -> short
            "UINT16" -> Schema.FieldType.INT32 // unsigned short -> int
            "UINT32" -> Schema.FieldType.INT64 // unsigned int -> long
            "UINT64" -> Schema.FieldType.DECIMAL // unsigned long -> decimal (may overflow)
            "UINT128", "UINT256" -> Schema.FieldType.DECIMAL

            // Floating point
            "FLOAT32", "FLOAT" -> Schema.FieldType.FLOAT
            "FLOAT64", "DOUBLE" -> Schema.FieldType.DOUBLE
            "DECIMAL", "DECIMAL32", "DECIMAL64", "DECIMAL128", "DECIMAL256" -> Schema.FieldType.DECIMAL

            // Boolean
            "BOOL", "BOOLEAN" -> Schema.FieldType.BOOLEAN

            // Date / Time
            "DATE" -> Schema.FieldType.DATETIME
            "DATE32" -> Schema.FieldType.DATETIME
            "DATETIME" -> Schema.FieldType.DATETIME
            "DATETIME64" -> Schema.FieldType.DATETIME
            "DATETIME32" -> Schema.FieldType.DATETIME

            // Binary
            "BINARY", "FIXEDBINARY", "BYTEA" -> Schema.FieldType.BYTES

            // Fallback: treat unknown types as STRING.
            else -> Schema.FieldType.STRING
        }
    }

    /** Whether the ClickHouse type name is nullable (case-insensitive). */
    fun isNullable(clickhouseTypeName: String): Boolean {
        val normalized = clickhouseTypeName.trim().uppercase()
        return normalized.startsWith("NULLABLE(")
    }
}
