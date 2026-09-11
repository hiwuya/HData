package me.jayer.hdata.hive.type

import me.jayer.hdata.core.type.FieldTypes
import me.jayer.hdata.hive.metastore.HiveColumn
import me.jayer.hdata.hive.metastore.HiveTable
import org.apache.beam.sdk.schemas.Schema

/**
 * Hive type string <-> Beam type.
 *
 * A column's type in the metastore is a string, with nested types written right inside it:
 * `array<struct<id:int,tags:map<string,decimal(10,2)>>>`. So it cannot be split on commas; the angle brackets must really be
 * parsed — that is also the root cause of the pre-refactor Hive connector **silently degrading to a single `value STRING`
 * column** when it could not infer a type.
 *
 * The mapping of time types follows Hive's semantics rather than guessing from the names:
 *  - `timestamp` in Hive is a **wall-clock time without time zone**, mapped to [FieldTypes.DATETIME] (`LocalDateTime`);
 *  - `timestamp with local time zone` is the point in time, mapped to [FieldTypes.TIMESTAMP] (`Instant`).
 * The reverse mapping (DATETIME -> `timestamp`) follows the same rule, so syncing across both ends does not gain a spurious
 *
 * @author wuya
 */
object HiveTypes {

    /** Turns a table's columns into a Beam schema. Partition columns come after the data columns, matching Hive's `SELECT *` order. */
    fun schemaOf(table: HiveTable): Schema = schemaOf(table.columns)

    fun schemaOf(columns: List<HiveColumn>): Schema = Schema.builder().apply {
        columns.forEach { column ->
            // Hive columns are always nullable, there is no NOT NULL constraint
            addField(Schema.Field.nullable(column.name, parse(column.type)))
        }
    }.build()

    /**
     * Parses one Hive type string.
     *
     * @throws IllegalArgumentException when the type is unknown or written illegally. **No fallback**: failing to infer one
     *   column's type fails the whole job, which is better than reading a pile of mismatched data.
     */
    fun parse(type: String): Schema.FieldType {
        val parser = TypeParser(type)
        val result = parser.parseType()
        parser.expectEnd()
        return result
    }

    /**
     * Beam type -> Hive type string, used when creating a table.
     *
     * `SqlTypes.TIME` has no matching Hive type (Hive still has no TIME), so it maps to `string` — better to store it explicitly
     * as a string than to create a table that cannot be read back.
     */
    fun toHiveType(fieldType: Schema.FieldType): String = when (fieldType.typeName) {
        Schema.TypeName.BYTE -> "tinyint"
        Schema.TypeName.INT16 -> "smallint"
        Schema.TypeName.INT32 -> "int"
        Schema.TypeName.INT64 -> "bigint"
        Schema.TypeName.FLOAT -> "float"
        Schema.TypeName.DOUBLE -> "double"
        Schema.TypeName.BOOLEAN -> "boolean"
        Schema.TypeName.STRING -> "string"
        Schema.TypeName.BYTES -> "binary"
        Schema.TypeName.DECIMAL -> "decimal(38,18)"
        Schema.TypeName.ARRAY, Schema.TypeName.ITERABLE ->
            "array<${toHiveType(fieldType.collectionElementType!!)}>"

        Schema.TypeName.MAP ->
            "map<${toHiveType(fieldType.mapKeyType!!)},${toHiveType(fieldType.mapValueType!!)}>"

        Schema.TypeName.ROW -> fieldType.rowSchema!!.fields.joinToString(
            separator = ",",
            prefix = "struct<",
            postfix = ">",
        ) { "${it.name}:${toHiveType(it.type)}" }

        Schema.TypeName.LOGICAL_TYPE -> when (fieldType.withNullable(false)) {
            FieldTypes.DATE -> "date"
            FieldTypes.DATETIME -> "timestamp"
            FieldTypes.TIMESTAMP -> "timestamp with local time zone"
            FieldTypes.TIME -> "string"
            else -> throw IllegalArgumentException("unsupported logical type: ${fieldType.logicalType?.identifier}")
        }

        else -> throw IllegalArgumentException("unsupported Beam type: $fieldType")
    }

    /** Precision and scale of a decimal, needed by the column declaration when writing ORC / Parquet. */
    fun decimalPrecisionAndScale(type: String): Pair<Int, Int> {
        val normalized = type.trim().lowercase()
        require(normalized.startsWith("decimal") || normalized.startsWith("numeric")) {
            "not a decimal type: $type"
        }
        val args = normalized.substringAfter('(', "").substringBefore(')')
        if (args.isBlank()) {
            // Hive 0.12 and earlier have decimal without parameters, equivalent to decimal(10,0)
            return 10 to 0
        }
        val parts = args.split(',').map { it.trim().toInt() }
        return when (parts.size) {
            1 -> parts[0] to 0
            2 -> parts[0] to parts[1]
            else -> throw IllegalArgumentException("illegal decimal parameter syntax: $type")
        }
    }

    /**
     * A hand-written recursive descent parser.
     *
     * Used instead of a regex: in `map<string,array<struct<a:int,b:string>>>` a comma has three different meanings, which a
     * regex cannot separate.
     */
    private class TypeParser(private val input: String) {

        private var pos = 0

        fun parseType(): Schema.FieldType {
            skipSpaces()
            val name = readIdentifier().lowercase()
            return when (name) {
                "tinyint", "byte" -> FieldTypes.BYTE
                "smallint", "short" -> FieldTypes.INT16
                "int", "integer" -> FieldTypes.INT32
                "bigint", "long" -> FieldTypes.INT64
                "float", "real" -> FieldTypes.FLOAT
                "double" -> FieldTypes.DOUBLE
                "boolean" -> FieldTypes.BOOLEAN
                "binary" -> FieldTypes.BYTES
                "date" -> FieldTypes.DATE
                "string" -> FieldTypes.STRING
                // Length parameters are meaningless on the Beam side, read them and discard
                "varchar", "char" -> FieldTypes.STRING.also { skipParenthesizedArgs() }
                "decimal", "numeric" -> FieldTypes.DECIMAL.also { skipParenthesizedArgs() }
                "timestamp" -> parseTimestamp()
                "array" -> Schema.FieldType.array(parseAngleBracketed { parseType() }.withNullable(true))
                "map" -> parseMap()
                "struct" -> parseStruct()
                "uniontype" -> throw IllegalArgumentException("Hive's uniontype is not supported yet: $input")
                "void" -> throw IllegalArgumentException("Hive's void type is not supported yet: $input")
                else -> throw IllegalArgumentException("unrecognized Hive type: $name (full type: $input)")
            }
        }

        /** `timestamp` / `timestamp with local time zone`; the latter is the time point carrying a time zone. */
        private fun parseTimestamp(): Schema.FieldType {
            val rest = input.substring(pos).trimStart().lowercase()
            if (rest.startsWith("with local time zone")) {
                pos = input.indexOf("zone", pos, ignoreCase = true) + "zone".length
                return FieldTypes.TIMESTAMP
            }
            return FieldTypes.DATETIME
        }

        private fun parseMap(): Schema.FieldType {
            expect('<')
            val keyType = parseType()
            skipSpaces()
            expect(',')
            val valueType = parseType()
            skipSpaces()
            expect('>')
            return Schema.FieldType.map(keyType, valueType.withNullable(true))
        }

        private fun parseStruct(): Schema.FieldType {
            expect('<')
            val builder = Schema.builder()
            while (true) {
                skipSpaces()
                val fieldName = readIdentifier()
                require(fieldName.isNotEmpty()) { "struct field name must not be empty (full type: $input)" }
                skipSpaces()
                expect(':')
                builder.addField(Schema.Field.nullable(fieldName, parseType()))
                skipSpaces()
                if (peek() == ',') {
                    pos++
                    continue
                }
                expect('>')
                return Schema.FieldType.row(builder.build())
            }
        }

        private fun <T> parseAngleBracketed(block: () -> T): T {
            expect('<')
            val result = block()
            skipSpaces()
            expect('>')
            return result
        }

        private fun readIdentifier(): String {
            skipSpaces()
            val start = pos
            while (pos < input.length && (input[pos].isLetterOrDigit() || input[pos] == '_')) {
                pos++
            }
            return input.substring(start, pos)
        }

        /** `(10,2)` / `(255)`; just consume it. */
        private fun skipParenthesizedArgs() {
            skipSpaces()
            if (peek() != '(') {
                return
            }
            val end = input.indexOf(')', pos)
            require(end > 0) { "unclosed parenthesis: $input" }
            pos = end + 1
        }

        private fun skipSpaces() {
            while (pos < input.length && input[pos].isWhitespace()) {
                pos++
            }
        }

        private fun peek(): Char? = if (pos < input.length) input[pos] else null

        private fun expect(c: Char) {
            skipSpaces()
            require(peek() == c) { "the type string should have '$c' at character $pos, but has '${peek() ?: "end"}' (full type: $input)" }
            pos++
        }

        fun expectEnd() {
            skipSpaces()
            require(pos >= input.length) { "the type string has trailing content: ${input.substring(pos)} (full type: $input)" }
        }
    }
}
