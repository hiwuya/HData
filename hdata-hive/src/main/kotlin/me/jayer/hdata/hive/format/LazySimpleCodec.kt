package me.jayer.hdata.hive.format

import me.jayer.hdata.hive.type.HiveValues
import org.apache.beam.sdk.schemas.Schema
import org.apache.beam.sdk.values.Row
import java.io.Serializable
import java.nio.charset.Charset
import java.time.Instant
import java.time.LocalDate
import java.time.LocalDateTime
import java.time.LocalTime
import java.util.Base64

/**
 * Row codec of Hive's `LazySimpleSerDe`, i.e. the rules turning one line of bytes into one line of values in TEXTFILE /
 * SEQUENCEFILE / RCFile(text). Implemented here instead of pulling in `hive-serde`: Hive's SerDe interface needs a whole set
 * of `ObjectInspector`, `Writable` and `Configuration` machinery, while all we need is "split by delimiter, parse by type" —
 * Trino writes its own too (`LineDeserializer`).
 *
 * Delimiters are **layered**, which is the easiest part to get wrong:
 *  - level 0 `field.delim` (default `\001`) separates columns;
 *  - level 1 `collection.delim` (default `\002`) separates array elements / struct fields / map entries;
 *  - level 2 `mapkey.delim` (default `\003`) separates a map's key from its value;
 *  - deeper levels continue with `\004`..`\010`.
 *
 * @author wuya
 */
class LazySimpleCodec(
    serdeParameters: Map<String, String>,
    private val charsetName: String = serdeParameters["serialization.encoding"] ?: "UTF-8",
) : Serializable {

    /** Eight delimiter levels, consistent with Hive's `LazySerDeParameters.separators`. */
    private val separators: CharArray = CharArray(8).also { separators ->
        separators[0] = firstChar(
            serdeParameters["field.delim"] ?: serdeParameters["serialization.format"],
            DEFAULT_FIELD_DELIM,
        )
        separators[1] = firstChar(
            // Hive historically misspelled this parameter as colelction.delim; accept both spellings
            serdeParameters["collection.delim"] ?: serdeParameters["colelction.delim"],
            DEFAULT_COLLECTION_DELIM,
        )
        separators[2] = firstChar(serdeParameters["mapkey.delim"], DEFAULT_MAPKEY_DELIM)
        for (i in 3 until separators.size) {
            separators[i] = (i + 1).toChar()
        }
    }

    private val nullSequence: String = serdeParameters["serialization.null.format"] ?: HiveValues.DEFAULT_NULL_FORMAT

    /** Escaping is only applied when `escape.delim` is configured; Hive does not escape by default. */
    private val escapeChar: Char? = serdeParameters["escape.delim"]?.takeIf { it.isNotEmpty() }?.first()

    /** The last column swallows the whole remainder (delimiters included), mirroring `serialization.last.column.takes.rest`. */
    private val lastColumnTakesRest: Boolean =
        serdeParameters["serialization.last.column.takes.rest"]?.toBoolean() ?: false

    val charset: Charset get() = Charset.forName(charsetName)

    val fieldDelimiter: Char get() = separators[0]

    /**
     * One line of text -> the projected column values.
     *
     * When there are fewer columns than in the table definition the missing ones are filled with null — that is Hive's semantics,
     */
    fun decodeRow(line: String, fieldTypes: List<Schema.FieldType>, projectedIndexes: List<Int>): Array<Any?> {
        val limit = if (lastColumnTakesRest) fieldTypes.size else 0
        val fields = line.split(separators[0], limit = limit)
        return Array(projectedIndexes.size) { i ->
            val index = projectedIndexes[i]
            fields.getOrNull(index)?.let { decodeField(it, fieldTypes[index], level = 1) }
        }
    }

    /**
     * The text of one field -> a value.
     *
     * @param level current nesting depth, decides which delimiter level is used
     */
    fun decodeField(text: String, fieldType: Schema.FieldType, level: Int): Any? {
        if (text == nullSequence) {
            return null
        }
        val target = fieldType.withNullable(false)
        return when (target.typeName) {
            Schema.TypeName.ARRAY, Schema.TypeName.ITERABLE -> {
                val elementType = target.collectionElementType!!
                if (text.isEmpty()) {
                    emptyList<Any?>()
                } else {
                    text.split(separatorAt(level)).map { decodeField(it, elementType, level + 1) }
                }
            }

            Schema.TypeName.MAP -> {
                val keyType = target.mapKeyType!!
                val valueType = target.mapValueType!!
                if (text.isEmpty()) {
                    emptyMap<Any?, Any?>()
                } else {
                    text.split(separatorAt(level)).mapNotNull { entry ->
                        val parts = entry.split(separatorAt(level + 1), limit = 2)
                        val key = decodeField(parts[0], keyType, level + 2) ?: return@mapNotNull null
                        key to parts.getOrNull(1)?.let { decodeField(it, valueType, level + 2) }
                    }.toMap()
                }
            }

            Schema.TypeName.ROW -> {
                val schema = target.rowSchema!!
                val parts = text.split(separatorAt(level))
                val builder = Row.withSchema(schema)
                schema.fields.forEachIndexed { i, field ->
                    builder.addValue(parts.getOrNull(i)?.let { decodeField(it, field.type, level + 1) })
                }
                builder.build()
            }

            // Hive stores binary as base64 in text formats
            Schema.TypeName.BYTES -> runCatching { Base64.getDecoder().decode(unescape(text)) }.getOrNull()

            else -> HiveValues.parseString(unescape(text), target)
        }
    }

    /** One line of values -> one line of text. Used by the write side. */
    fun encodeRow(row: Row): String = row.schema.fields.indices.joinToString(separators[0].toString()) { i ->
        encodeField(row.getValue<Any?>(i), row.schema.getField(i).type, level = 1)
    }

    fun encodeField(value: Any?, fieldType: Schema.FieldType, level: Int): String {
        if (value == null) {
            return nullSequence
        }
        val target = fieldType.withNullable(false)
        return when (target.typeName) {
            Schema.TypeName.ARRAY, Schema.TypeName.ITERABLE ->
                (value as Iterable<*>).joinToString(separatorAt(level).toString()) {
                    encodeField(it, target.collectionElementType!!, level + 1)
                }

            Schema.TypeName.MAP -> (value as Map<*, *>).entries.joinToString(separatorAt(level).toString()) { (k, v) ->
                encodeField(k, target.mapKeyType!!, level + 2) +
                    separatorAt(level + 1) +
                    encodeField(v, target.mapValueType!!, level + 2)
            }

            Schema.TypeName.ROW -> {
                val nested = value as Row
                nested.schema.fields.indices.joinToString(separatorAt(level).toString()) { i ->
                    encodeField(nested.getValue<Any?>(i), nested.schema.getField(i).type, level + 1)
                }
            }

            Schema.TypeName.BYTES -> Base64.getEncoder().encodeToString(value as ByteArray)

            else -> escape(toHiveText(value))
        }
    }

    /**
     * A value -> Hive's text literal.
     *
     * timestamp uses `yyyy-MM-dd HH:mm:ss[.fff]` (a space in the middle) rather than ISO-8601's `T` — written with `T`, Hive
     * itself reads it back as NULL.
     */
    private fun toHiveText(value: Any): String = when (value) {
        is LocalDateTime -> value.toString().replace('T', ' ')
        is Instant -> LocalDateTime.ofInstant(value, java.time.ZoneOffset.UTC).toString().replace('T', ' ')
        is LocalDate, is LocalTime -> value.toString()
        else -> value.toString()
    }

    private fun separatorAt(level: Int): Char = separators[level.coerceAtMost(separators.size - 1)]

    private fun unescape(text: String): String {
        val escape = escapeChar ?: return text
        if (!text.contains(escape)) {
            return text
        }
        val sb = StringBuilder(text.length)
        var i = 0
        while (i < text.length) {
            if (text[i] == escape && i + 1 < text.length) {
                sb.append(text[i + 1])
                i += 2
            } else {
                sb.append(text[i])
                i++
            }
        }
        return sb.toString()
    }

    private fun escape(text: String): String {
        val escape = escapeChar ?: return text
        val sb = StringBuilder(text.length)
        text.forEach { c ->
            if (c == escape || separators.contains(c)) {
                sb.append(escape)
            }
            sb.append(c)
        }
        return sb.toString()
    }

    private fun firstChar(value: String?, fallback: Char): Char =
        value?.takeIf { it.isNotEmpty() }?.first() ?: fallback

    companion object {
        private const val serialVersionUID: Long = 1

        /** When a Hive table is created without ROW FORMAT, these three default delimiters apply: ^A / ^B / ^C. */
        const val DEFAULT_FIELD_DELIM = '\u0001'
        const val DEFAULT_COLLECTION_DELIM = '\u0002'
        const val DEFAULT_MAPKEY_DELIM = '\u0003'
    }
}
