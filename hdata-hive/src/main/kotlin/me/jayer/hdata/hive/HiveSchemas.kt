package me.jayer.hdata.hive

import org.apache.beam.sdk.schemas.Schema
import org.slf4j.LoggerFactory
import java.sql.ResultSet

/**
 * Hive 字段类型到 Beam 字段类型的映射与 schema 构建辅助。
 *
 * 输出 schema 可由显式的 `name:TYPE` 列表定义，也可在构图阶段用 `DESCRIBE` 推断；
 * 二者都拿不到时回退成单列 `value`(STRING)。
 */
enum class HiveFieldType {
    STRING, INT32, INT64, DOUBLE, BOOLEAN, DATETIME, BYTES
}

/** 把单个 `name:TYPE` 片段解析成 Beam 字段（必填字段）。 */
fun parseField(field: String): Schema.Field {
    val (name, type) = field.split(":", limit = 2).let { it[0].trim() to it.getOrNull(1)?.trim() }
    require(name.isNotBlank()) { "schema_fields 条项格式非法: $field" }
    return Schema.Field.of(name, toFieldType(type).withNullable(true))
}

private fun toFieldType(type: String?): Schema.FieldType = when (type?.uppercase()) {
    null, "", "STRING" -> Schema.FieldType.STRING
    "INT32", "INT" -> Schema.FieldType.INT32
    "INT64", "BIGINT", "LONG" -> Schema.FieldType.INT64
    "DOUBLE", "FLOAT" -> Schema.FieldType.DOUBLE
    "BOOLEAN", "BOOL" -> Schema.FieldType.BOOLEAN
    "DATETIME", "TIMESTAMP" -> Schema.FieldType.DATETIME
    "BYTES", "BINARY" -> Schema.FieldType.BYTES
    else -> throw IllegalArgumentException("不支持的 Hive 字段类型: $type")
}

/** 由显式 `schema_fields` 构建 schema。 */
fun buildSchemaFromFields(fields: List<String>): Schema? = if (fields.isEmpty()) {
    null
} else {
    val builder = Schema.builder()
    fields.forEach { builder.addField(parseField(it)) }
    builder.build()
}

/** 由 `DESCRIBE <db>.<table>` 推断列名与类型。 */
fun deriveSchema(connection: java.sql.Connection, database: String, table: String): Schema? = runCatching {
    val qualified = if (database.isNotBlank()) "$database.$table" else table
    connection.createStatement().use { st ->
        st.executeQuery("DESCRIBE $qualified").use { rs ->
            val builder = Schema.builder()
            var any = false
            while (rs.next()) {
                val colName = rs.getString("col_name") ?: continue
                if (colName.isBlank()) continue
                val colType = rs.getString("data_type") ?: "string"
                builder.addField(Schema.Field.of(colName, hiveTypeToFieldType(colType).withNullable(true)))
                any = true
            }
            if (!any) null else builder.build()
        }
    }
}.getOrNull()

private fun hiveTypeToFieldType(hiveType: String): Schema.FieldType {
    val base = hiveType.lowercase().substringBefore("(").trim()
    return when (base) {
        "string", "varchar", "char" -> Schema.FieldType.STRING
        "tinyint", "smallint", "int", "integer" -> Schema.FieldType.INT32
        "bigint" -> Schema.FieldType.INT64
        "float", "double", "decimal" -> Schema.FieldType.DOUBLE
        "boolean", "bool" -> Schema.FieldType.BOOLEAN
        "timestamp", "date", "timestampwithlocaltimezone" -> Schema.FieldType.DATETIME
        "binary" -> Schema.FieldType.BYTES
        else -> Schema.FieldType.STRING
    }
}

/** 列名列表（按 schema 顺序）。 */
fun Schema.columnNames(): List<String> = (0 until fieldCount).map { getField(it).name }

/**
 * 把 ResultSet 当前行按 [schema] 映射成 Beam Row。各列按 schema 字段顺序对齐，
 * 类型转换失败或为空时用 null（字段可空），保证稳健。
 */
fun resultSetToRow(schema: Schema, rs: ResultSet): org.apache.beam.sdk.values.Row {
    val rowBuilder = org.apache.beam.sdk.values.Row.withSchema(schema)
    for (i in 0 until schema.fieldCount) {
        val field = schema.getField(i)
        val sqlIndex = i + 1
        rowBuilder.addValue(
            when (field.type.typeName) {
                Schema.TypeName.STRING -> rs.getString(sqlIndex)
                Schema.TypeName.INT32 -> rs.getInt(sqlIndex).takeIf { !rs.wasNull() }
                Schema.TypeName.INT64 -> rs.getLong(sqlIndex).takeIf { !rs.wasNull() }
                Schema.TypeName.DOUBLE -> rs.getDouble(sqlIndex).takeIf { !rs.wasNull() }
                Schema.TypeName.BOOLEAN -> rs.getBoolean(sqlIndex).takeIf { !rs.wasNull() }
                Schema.TypeName.DATETIME -> rs.getTimestamp(sqlIndex)?.let { org.joda.time.Instant(it.time) }
                Schema.TypeName.BYTES -> rs.getBytes(sqlIndex)
                else -> rs.getObject(sqlIndex)
            },
        )
    }
    return rowBuilder.build()
}

private val LOGGER = LoggerFactory.getLogger("me.jayer.hdata.hive.HiveSchemas")
