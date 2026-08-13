package me.jayer.hdata.hbase

import org.apache.beam.sdk.schemas.Schema
import org.apache.hadoop.hbase.util.Bytes

/**
 * `schema_fields` 的解析，以及 HBase 单元格字节与 Beam 字段值的双向转换。
 *
 * 条目支持两种写法：
 *  - `qualifier:type` —— 列族取配置里的 `family`；
 *  - `family:qualifier:type` —— 显式指定列族，这样一次可以读写多个列族。
 *
 * 重构前只认第一种，等于把"所有列都在同一个列族"写死进了实现。
 *
 * @author wuya
 */
data class HBaseColumn(
    val family: String,
    val qualifier: String,
    val type: HBaseType,
) : java.io.Serializable {
    /** Beam 行里的字段名。多列族时带上列族前缀，避免不同列族的同名列撞车。 */
    val fieldName: String get() = qualifier

    val familyBytes: ByteArray get() = Bytes.toBytes(family)
    val qualifierBytes: ByteArray get() = Bytes.toBytes(qualifier)

    companion object {
        fun parse(spec: String, defaultFamily: String): HBaseColumn {
            val parts = spec.split(':').map { it.trim() }
            val (family, qualifier, type) = when (parts.size) {
                1 -> Triple(defaultFamily, parts[0], HBaseType.STRING.name)
                2 -> Triple(defaultFamily, parts[0], parts[1])
                3 -> Triple(parts[0], parts[1], parts[2])
                else -> throw IllegalArgumentException(
                    "schema_fields 条目格式应为 qualifier:type 或 family:qualifier:type，收到: $spec"
                )
            }
            require(qualifier.isNotBlank()) { "schema_fields 条目的列名不能为空: $spec" }
            require(family.isNotBlank()) { "schema_fields 条目的列族不能为空: $spec" }
            return HBaseColumn(family, qualifier, HBaseType.of(type))
        }
    }
}

/** HBase 里以字节存储的值与 Beam 字段类型的对应关系。 */
enum class HBaseType(val fieldType: Schema.FieldType, private val width: Int?) {

    STRING(Schema.FieldType.STRING, null),
    INT32(Schema.FieldType.INT32, Bytes.SIZEOF_INT),
    INT64(Schema.FieldType.INT64, Bytes.SIZEOF_LONG),
    DOUBLE(Schema.FieldType.DOUBLE, Bytes.SIZEOF_DOUBLE),
    BOOLEAN(Schema.FieldType.BOOLEAN, Bytes.SIZEOF_BOOLEAN),
    BYTES(Schema.FieldType.BYTES, null),
    ;

    /**
     * 定长类型遇到宽度对不上的单元格时给出能定位问题的报错。
     *
     * `Bytes.toInt` 对短于 4 字节的输入会抛 `IllegalArgumentException`，长于 4 字节则**静默只取前 4 字节**——
     * 后者最阴险：数据是错的但作业照常成功。这里两种情况都拦下来。
     */
    fun decode(column: HBaseColumn, bytes: ByteArray?): Any? {
        if (bytes == null) {
            return null
        }
        if (width != null && bytes.size != width) {
            throw IllegalArgumentException(
                "列[${column.family}:${column.qualifier}] 声明为 $name（$width 字节），" +
                    "实际单元格有 ${bytes.size} 字节。它多半不是用 Bytes.toBytes 写进去的，" +
                    "改用 STRING 或 BYTES 类型再自行解析"
            )
        }
        return when (this) {
            STRING -> Bytes.toString(bytes)
            INT32 -> Bytes.toInt(bytes)
            INT64 -> Bytes.toLong(bytes)
            DOUBLE -> Bytes.toDouble(bytes)
            BOOLEAN -> Bytes.toBoolean(bytes)
            BYTES -> bytes
        }
    }

    fun encode(column: HBaseColumn, value: Any?): ByteArray? {
        if (value == null) {
            return null
        }
        return try {
            when (this) {
                STRING -> Bytes.toBytes(value as String)
                INT32 -> Bytes.toBytes((value as Number).toInt())
                INT64 -> Bytes.toBytes((value as Number).toLong())
                DOUBLE -> Bytes.toBytes((value as Number).toDouble())
                BOOLEAN -> Bytes.toBytes(value as Boolean)
                BYTES -> value as ByteArray
            }
        } catch (e: ClassCastException) {
            throw IllegalArgumentException(
                "列[${column.family}:${column.qualifier}] 声明为 $name，" +
                    "但输入行里这个字段是 ${value.javaClass.simpleName}",
                e,
            )
        }
    }

    companion object {
        fun of(name: String): HBaseType = entries.firstOrNull { it.name == name.uppercase() }
            ?: throw IllegalArgumentException(
                "schema_fields 不支持的类型: $name，可选 ${entries.joinToString { it.name }}"
            )
    }
}

/** rowkey 在 Beam 行里的呈现方式。二进制 rowkey 用 STRING 会被破坏，必须用 BYTES。 */
enum class RowkeyFormat(val configValue: String, val fieldType: Schema.FieldType) {

    STRING("string", Schema.FieldType.STRING),
    BYTES("bytes", Schema.FieldType.BYTES),
    ;

    fun decode(rowkey: ByteArray): Any = if (this == STRING) Bytes.toString(rowkey) else rowkey

    fun encode(value: Any?): ByteArray {
        checkNotNull(value) { "rowkey 不能为 null" }
        return when {
            this == BYTES -> value as? ByteArray
                ?: throw IllegalArgumentException("rowkey_format=bytes 要求 rowkey 字段是 BYTES，实际是 ${value.javaClass.simpleName}")
            // 重构前这里是 Bytes.toBytes(value.toString())，Long 会被写成十进制字符串、
            // ByteArray 会被写成 "[B@1a2b3c"，读回来对不上
            value is String -> Bytes.toBytes(value)
            else -> throw IllegalArgumentException(
                "rowkey_format=string 要求 rowkey 字段是 STRING，实际是 ${value.javaClass.simpleName}；" +
                    "二进制 rowkey 请设 rowkey_format: bytes"
            )
        }
    }

    companion object {
        fun of(value: String): RowkeyFormat = entries.firstOrNull { it.configValue == value }
            ?: throw IllegalArgumentException(
                "rowkey_format 取值非法: $value，可选 ${entries.joinToString { it.configValue }}"
            )
    }
}

fun parseColumns(schemaFields: List<String>?, defaultFamily: String): List<HBaseColumn> =
    schemaFields?.map { HBaseColumn.parse(it, defaultFamily) } ?: emptyList()

/** 读出行的 schema：rowkey 在最前，之后按 [columns] 顺序。 */
fun buildReadSchema(rowkeyField: String, rowkeyFormat: RowkeyFormat, columns: List<HBaseColumn>): Schema {
    val builder = Schema.builder().addField(rowkeyField, rowkeyFormat.fieldType)
    columns.forEach { builder.addNullableField(it.fieldName, it.type.fieldType) }
    return builder.build()
}
