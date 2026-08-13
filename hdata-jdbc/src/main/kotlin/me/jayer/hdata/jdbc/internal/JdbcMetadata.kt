package me.jayer.hdata.jdbc.internal

import org.apache.beam.sdk.schemas.Schema
import org.slf4j.LoggerFactory
import java.sql.Connection
import java.sql.ResultSet

/**
 * 构图期的元数据探测：查询 schema、主键、分区列取值范围。
 *
 * @author wuya
 * @date 2022-08-04
 */
internal object JdbcMetadata {

    private val LOGGER = LoggerFactory.getLogger(JdbcMetadata::class.java)

    /**
     * 不执行查询、只取结果集元数据。
     *
     * 少数驱动在未执行前返回 null，此时退回执行一次并把 `maxRows` 设成 1。
     */
    fun describe(connection: Connection, sql: String): List<JdbcColumn> {
        connection.prepareStatement(sql).use { ps ->
            ps.metaData?.let { metaData ->
                return (1..metaData.columnCount).map { JdbcColumn.from(metaData, it) }
            }
            LOGGER.debug("驱动未提供预编译元数据，退回执行一次: {}", sql)
            ps.maxRows = 1
            ps.executeQuery().use { rs ->
                val metaData = rs.metaData
                return (1..metaData.columnCount).map { JdbcColumn.from(metaData, it) }
            }
        }
    }

    /**
     * 把 JDBC 列元数据翻译成 Beam schema，同时解析出每列的读取方式。
     *
     * 列名重复（例如 join 了两张有同名列的表）在 Beam 侧会抛一句很难懂的错，这里提前拦下并提示用别名。
     */
    fun toSchema(columns: List<JdbcColumn>): Pair<Schema, List<ResultSetReader>> {
        require(columns.isNotEmpty()) { "查询没有返回任何列" }

        val duplicated = columns.groupingBy { it.label }.eachCount().filterValues { it > 1 }.keys
        require(duplicated.isEmpty()) {
            "查询结果里有重名列 $duplicated，请在 SQL 里用别名区分（例如 SELECT a.id AS a_id, b.id AS b_id）"
        }

        val codecs = columns.map { column ->
            requireNotNull(TypeMappings.resolve(column)) {
                "列 ${column.describe()} 的类型暂不支持，可在 SQL 里先转成字符串再同步"
            }
        }
        val schema = Schema.builder()
            .addFields(columns.zip(codecs) { column, codec ->
                Schema.Field.of(column.label, codec.fieldType).withNullable(column.nullable)
            })
            .build()
        return schema to codecs.map { it.reader }
    }

    fun describeTable(connection: Connection, table: String): List<JdbcColumn> =
        describe(connection, SelectSql(table).render())

    /**
     * 按 `KEY_SEQ` 顺序返回主键列。
     *
     * 两个坑：
     * 1. `getPrimaryKeys` 返回的行**不保证有序**，H2 就会先给出 KEY_SEQ=2 那列。重构前直接取
     *    `firstOrNull()`，复合主键下会挑中错误的列。
     * 2. 表名大小写必须和字典里存的一致：H2 / Oracle 存大写，PostgreSQL 存小写，
     *    传错了只会得到空结果，于是自动分区**悄悄不生效**，作业退化成单线程读且没有任何提示。
     */
    fun primaryKeyColumns(connection: Connection, table: String): List<String> {
        val (schema, name) = TableNames.split(table)
        for (candidate in nameVariants(name)) {
            val keys = readPrimaryKeys(connection, schema, candidate)
            if (keys.isNotEmpty()) return keys
        }
        return emptyList()
    }

    private fun nameVariants(name: String): List<String> =
        listOf(name, name.uppercase(), name.lowercase()).distinct()

    private fun readPrimaryKeys(connection: Connection, schema: String?, table: String): List<String> =
        runCatching {
            connection.metaData.getPrimaryKeys(connection.catalog, schema, table).use { rs ->
                buildList {
                    while (rs.next()) {
                        add(rs.getShort("KEY_SEQ") to rs.getString("COLUMN_NAME"))
                    }
                }
            }.sortedBy { it.first }.map { it.second }
        }.getOrElse {
            LOGGER.debug("读取表[{}]主键失败: {}", table, it.message)
            emptyList()
        }

    /** 分区列的取值范围，空表返回 (null, null)。 */
    fun partitionRange(connection: Connection, select: SelectSql, column: String): Pair<Any?, Any?> {
        val sql = select.withColumns("min($column)", "max($column)").render()
        connection.prepareStatement(sql).use { ps ->
            ps.executeQuery().use { rs ->
                if (!rs.next()) return null to null
                return rs.getObject(1) to rs.getObject(2)
            }
        }
    }

    /** 分区列上有多少 NULL——它们会被 `col >= ? AND col < ?` 过滤掉，属于静默丢数据。 */
    fun countNulls(connection: Connection, select: SelectSql, column: String): Long {
        val sql = select.withColumns("count(*)").withConditions("$column IS NULL").render()
        connection.prepareStatement(sql).use { ps ->
            ps.executeQuery().use { rs ->
                return if (rs.next()) rs.getLong(1) else 0
            }
        }
    }

    /** 供 [countNulls] 之外的地方复用的通用单值查询。 */
    fun <T> queryOne(connection: Connection, sql: String, extract: (ResultSet) -> T): T? {
        connection.prepareStatement(sql).use { ps ->
            ps.executeQuery().use { rs ->
                return if (rs.next()) extract(rs) else null
            }
        }
    }
}
