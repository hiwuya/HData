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
 * Hive `LazySimpleSerDe` 的行编解码，也就是 TEXTFILE / SEQUENCEFILE / RCFile(text) 里
 * 一行字节到一行值的那套规则。自己实现而不是引 `hive-serde`：
 * Hive 的 SerDe 接口要 `ObjectInspector`、`Writable`、`Configuration` 一整套东西，
 * 而这里只需要"按分隔符切开、按类型解析"，Trino 也是自己写的（`LineDeserializer`）。
 *
 * 分隔符是**分层**的，这是最容易写错的地方：
 *  - 第 0 层 `field.delim`（默认 `\001`）分列；
 *  - 第 1 层 `collection.delim`（默认 `\002`）分数组元素 / struct 字段 / map 条目；
 *  - 第 2 层 `mapkey.delim`（默认 `\003`）分 map 的 key 和 value；
 *  - 更深的层级依次是 `\004`..`\010`。
 *
 * @author wuya
 */
class LazySimpleCodec(
    serdeParameters: Map<String, String>,
    private val charsetName: String = serdeParameters["serialization.encoding"] ?: "UTF-8",
) : Serializable {

    /** 8 层分隔符，与 Hive `LazySerDeParameters.separators` 一致。 */
    private val separators: CharArray = CharArray(8).also { separators ->
        separators[0] = firstChar(
            serdeParameters["field.delim"] ?: serdeParameters["serialization.format"],
            DEFAULT_FIELD_DELIM,
        )
        separators[1] = firstChar(
            // Hive 历史上把这个参数名拼错成 colelction.delim，两个都认
            serdeParameters["collection.delim"] ?: serdeParameters["colelction.delim"],
            DEFAULT_COLLECTION_DELIM,
        )
        separators[2] = firstChar(serdeParameters["mapkey.delim"], DEFAULT_MAPKEY_DELIM)
        for (i in 3 until separators.size) {
            separators[i] = (i + 1).toChar()
        }
    }

    private val nullSequence: String = serdeParameters["serialization.null.format"] ?: HiveValues.DEFAULT_NULL_FORMAT

    /** `escape.delim` 配了才做转义处理，Hive 默认不转义。 */
    private val escapeChar: Char? = serdeParameters["escape.delim"]?.takeIf { it.isNotEmpty() }?.first()

    /** 最后一列吞掉剩下的全部内容（含分隔符），对应 `serialization.last.column.takes.rest`。 */
    private val lastColumnTakesRest: Boolean =
        serdeParameters["serialization.last.column.takes.rest"]?.toBoolean() ?: false

    val charset: Charset get() = Charset.forName(charsetName)

    val fieldDelimiter: Char get() = separators[0]

    /**
     * 一行文本 -> 投影到的列值。
     *
     * 列数比表定义少时，缺的列补 null——Hive 就是这个语义，加列之后的老文件全靠它才读得动。
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
     * 一个字段的文本 -> 值。
     *
     * @param level 当前嵌套深度，决定用哪一层分隔符
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

            // Hive 在文本格式里把 binary 存成 base64
            Schema.TypeName.BYTES -> runCatching { Base64.getDecoder().decode(unescape(text)) }.getOrNull()

            else -> HiveValues.parseString(unescape(text), target)
        }
    }

    /** 一行值 -> 一行文本。写入端用。 */
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
     * 值 -> Hive 的文本字面量。
     *
     * timestamp 用 `yyyy-MM-dd HH:mm:ss[.fff]`（中间是空格）而不是 ISO-8601 的 `T`——
     * 写成 `T` 的话 Hive 自己读回去是 NULL。
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

        /** Hive 建表时不写 ROW FORMAT 就是这三个默认分隔符：^A / ^B / ^C。 */
        const val DEFAULT_FIELD_DELIM = '\u0001'
        const val DEFAULT_COLLECTION_DELIM = '\u0002'
        const val DEFAULT_MAPKEY_DELIM = '\u0003'
    }
}
