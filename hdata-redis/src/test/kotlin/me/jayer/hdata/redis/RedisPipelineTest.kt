package me.jayer.hdata.redis

import me.jayer.hdata.core.spec.ErrorHandlingSpec
import me.jayer.hdata.core.spec.SpecMappers
import me.jayer.hdata.core.spi.Tags
import me.jayer.hdata.core.spi.TransformConfig
import org.apache.beam.sdk.Pipeline
import org.apache.beam.sdk.schemas.Schema
import org.apache.beam.sdk.testing.PAssert
import org.apache.beam.sdk.transforms.Count
import org.apache.beam.sdk.transforms.Create
import org.apache.beam.sdk.values.PCollectionRowTuple
import org.apache.beam.sdk.values.Row
import me.jayer.hdata.redis.transform.REDIS_KEY_VALUE_SCHEMA
import org.redisson.Redisson
import org.redisson.api.RedissonClient
import org.redisson.api.StreamMessageId
import org.redisson.api.stream.StreamAddArgs
import org.redisson.client.codec.StringCodec
import org.redisson.config.Config
import redis.embedded.RedisServer
import tools.jackson.databind.node.ObjectNode
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/**
 * End-to-end tests for `ReadFromRedis` / `WriteToRedis`: run the full chain against a real in-process
 * Redis (embedded-redis).
 *
 * Write tests do not assert Redis's internal data structures directly; instead they check the written
 * result with a RedissonClient against the same embedded server. Read tests write data first, then read
 * it back, verifying the scan / keys / stream modes.
 */
class RedisPipelineTest {

    private fun startServer(): Pair<RedisServer, Int> {
        val port = java.net.ServerSocket(0).use { it.localPort }
        val server = RedisServer.newRedisServer().port(port).build()
        server.start()
        return server to port
    }

    private fun client(host: String, port: Int): RedissonClient =
        Redisson.create(
            Config().apply {
                useSingleServer().also { it.address = "redis://$host:$port" }
            }
        )

    private fun makeCfg(name: String, yaml: String, withErrorHandling: Boolean = false): TransformConfig {
        val node = SpecMappers.YAML.readTree(yaml) as ObjectNode
        return TransformConfig(name, node, if (withErrorHandling) ErrorHandlingSpec(output = "errors") else null)
    }

    private fun row(schema: Schema, key: String, value: String, field: String = "f") =
        Row.withSchema(schema).addValue(key).addValue(value).addValue(field).build()

    @Test
    fun `writes with set and reads it back via scan`() {
        val (server, port) = startServer()
        val host = "localhost"
        val schema = Schema.builder().addStringField("key").addStringField("value").addStringField("field").build()
        val rows = listOf(row(schema, "k1", "v1"), row(schema, "k2", "v2"))

        val writePipeline = Pipeline.create()
        val input = writePipeline.apply(Create.of(rows).withRowSchema(schema))
        PCollectionRowTuple.of(Tags.MAIN_INPUT, input).apply(
            RedisWriteProvider().from(
                makeCfg(
                    "WriteToRedis",
                    """
                    host: "$host"
                    port: $port
                    mode: set
                    key_field: key
                    value_field: value
                    """.trimIndent(),
                )
            )
        )
        writePipeline.run().waitUntilFinish()

        val c = client(host, port)
        try {
            assertEquals("v1", c.getBucket<String>("k1", StringCodec.INSTANCE).get())
            assertEquals("v2", c.getBucket<String>("k2", StringCodec.INSTANCE).get())
        } finally {
            c.shutdown()
        }

        val readPipeline = Pipeline.create()
        val out = PCollectionRowTuple.empty(readPipeline).apply(
            RedisReadProvider().from(
                makeCfg(
                    "ReadFromRedis",
                    """
                    host: "$host"
                    port: $port
                    mode: scan
                    key_pattern: "k*"
                    """.trimIndent(),
                )
            )
        )
        PAssert.that(out.get(Tags.MAIN_OUTPUT)).containsInAnyOrder(
            Row.withSchema(REDIS_KEY_VALUE_SCHEMA).addValue("k1").addValue("v1").build(),
            Row.withSchema(REDIS_KEY_VALUE_SCHEMA).addValue("k2").addValue("v2").build(),
        )
        readPipeline.run().waitUntilFinish()
        server.stop()
    }

    @Test
    fun `keys mode reads only the specified keys`() {
        val (server, port) = startServer()
        val host = "localhost"
        val c = client(host, port)
        try {
            c.getBucket<String>("a", StringCodec.INSTANCE).set("1")
            c.getBucket<String>("b", StringCodec.INSTANCE).set("2")
            c.getBucket<String>("c", StringCodec.INSTANCE).set("3")
        } finally {
            c.shutdown()
        }
        val schema = Schema.builder().addNullableField("key", Schema.FieldType.STRING)
            .addNullableField("value", Schema.FieldType.STRING).build()
        val readPipeline = Pipeline.create()
        val out = PCollectionRowTuple.empty(readPipeline).apply(
            RedisReadProvider().from(
                makeCfg(
                    "ReadFromRedis",
                    """
                    host: "$host"
                    port: $port
                    mode: keys
                    keys: ["a", "c"]
                    """.trimIndent(),
                )
            )
        )
        PAssert.that(out.get(Tags.MAIN_OUTPUT)).containsInAnyOrder(
            Row.withSchema(schema).addValue("a").addValue("1").build(),
            Row.withSchema(schema).addValue("c").addValue("3").build(),
        )
        readPipeline.run().waitUntilFinish()
        server.stop()
    }

    @Test
    fun `lpush rpush sadd hset each land correctly`() {
        val (server, port) = startServer()
        val host = "localhost"
        val schema = Schema.builder().addStringField("key").addStringField("value").addStringField("field").build()

        fun write(mode: String, data: List<Row>) {
            val p = Pipeline.create()
            val input = p.apply(Create.of(data).withRowSchema(schema))
            PCollectionRowTuple.of(Tags.MAIN_INPUT, input).apply(
                RedisWriteProvider().from(
                    makeCfg("WriteToRedis", "host: \"$host\"\nport: $port\nmode: $mode\nkey_field: key\nvalue_field: value\nhash_field: field\n")
                )
            )
            p.run().waitUntilFinish()
        }
        write("lpush", listOf(row(schema, "list", "x"), row(schema, "list", "y")))
        write("sadd", listOf(row(schema, "set", "s")))
        write("hset", listOf(Row.withSchema(schema).addValue("hash").addValue("hv").addValue("hf").build()))

        val c = client(host, port)
        try {
            assertEquals(setOf("x", "y"), c.getList<String>("list", StringCodec.INSTANCE).toSet())
            assertEquals(setOf("s"), c.getSet<String>("set", StringCodec.INSTANCE).toSet())
            assertEquals("hv", c.getMap<String, String>("hash", StringCodec.INSTANCE)["hf"])
        } finally {
            c.shutdown()
        }
        server.stop()
    }

    @Test
    fun `non-set mode data changes take effect together with TTL`() {
        val (server, port) = startServer()
        val schema = Schema.builder().addStringField("key").addStringField("value").addStringField("field").build()

        listOf("lpush", "rpush", "sadd", "hset").forEach { mode ->
            val key = "ttl-$mode"
            val p = Pipeline.create()
            val input = p.apply(Create.of(row(schema, key, "v", "f")).withRowSchema(schema))
            PCollectionRowTuple.of(Tags.MAIN_INPUT, input).apply(
                RedisWriteProvider().from(
                    makeCfg(
                        "WriteToRedis",
                        "host: localhost\nport: $port\nmode: $mode\nttl_seconds: 60\n",
                    ),
                ),
            )
            p.run().waitUntilFinish()
        }

        val c = client("localhost", port)
        try {
            listOf("lpush", "rpush", "sadd", "hset").forEach { mode ->
                assertTrue(c.keys.remainTimeToLive("ttl-$mode") > 0, "$mode should carry a TTL after writing")
            }
        } finally {
            c.shutdown()
            server.stop()
        }
    }

    @Test
    fun `stream mode expands entries into multiple rows`() {
        val (server, port) = startServer()
        val host = "localhost"
        val stream = "mystream"
        val c = client(host, port)
        try {
            val s = c.getStream<String, String>(stream, StringCodec.INSTANCE)
            s.add(StreamMessageId.AUTO_GENERATED, StreamAddArgs.entries("name", "alice", "age", "30"))
            s.add(StreamMessageId.AUTO_GENERATED, StreamAddArgs.entries("name", "bob", "age", "40"))
        } finally {
            c.shutdown()
        }
        val schema = Schema.builder().addNullableField("id", Schema.FieldType.STRING)
            .addNullableField("field", Schema.FieldType.STRING)
            .addNullableField("value", Schema.FieldType.STRING).build()
        val readPipeline = Pipeline.create()
        val out = PCollectionRowTuple.empty(readPipeline).apply(
            RedisReadProvider().from(
                makeCfg(
                    "ReadFromRedis",
                    """
                    host: "$host"
                    port: $port
                    mode: stream
                    stream: "$stream"
                    """.trimIndent(),
                )
            )
        )
        PAssert.that(out.get(Tags.MAIN_OUTPUT)).satisfies { output ->
            val rows = output.toList()
            assertEquals(4, rows.size)
            val pairs = rows.map { it.getString("field") to it.getString("value") }.toSet()
            assertEquals(
                setOf("name" to "alice", "age" to "30", "name" to "bob", "age" to "40"),
                pairs,
            )
            null
        }
        readPipeline.run().waitUntilFinish()
        server.stop()
    }

    @Test
    fun `stream mode's start_id and end_id really do bound the range`() {
        // These two parameters used to be accepted and then silently dropped: no matter what you set them
        // to, the read always spanned MIN..MAX, so the user thought they were reading an increment while
        // every read was actually a full scan
        val (server, port) = startServer()
        val host = "localhost"
        val stream = "ranged"
        val ids = mutableListOf<StreamMessageId>()
        val c = client(host, port)
        try {
            val s = c.getStream<String, String>(stream, StringCodec.INSTANCE)
            // only the single-argument add() returns the auto-generated entry id
            repeat(3) { i -> ids += s.add(StreamAddArgs.entry("seq", i.toString())) }
        } finally {
            c.shutdown()
        }

        // take only the middle entry
        val middle = ids[1]
        val readPipeline = Pipeline.create()
        val out = PCollectionRowTuple.empty(readPipeline).apply(
            RedisReadProvider().from(
                makeCfg(
                    "ReadFromRedis",
                    """
                    host: "$host"
                    port: $port
                    mode: stream
                    stream: "$stream"
                    start_id: "${middle.id0}-${middle.id1}"
                    end_id: "${middle.id0}-${middle.id1}"
                    """.trimIndent(),
                )
            )
        )
        PAssert.that(out.get(Tags.MAIN_OUTPUT)).satisfies { output ->
            val rows = output.toList()
            assertEquals(1, rows.size, "bounding the range should read back only the middle entry")
            assertEquals("1", rows.single().getString("value"))
            null
        }
        readPipeline.run().waitUntilFinish()
        server.stop()
    }

    @Test
    fun `a row missing the key field goes to the dead letter`() {
        val (server, port) = startServer()
        val host = "localhost"
        val schema = Schema.builder().addStringField("value").build()
        val rows = listOf(Row.withSchema(schema).addValue("v").build())
        val p = Pipeline.create()
        val input = p.apply(Create.of(rows).withRowSchema(schema))
        val out = PCollectionRowTuple.of(Tags.MAIN_INPUT, input).apply(
            RedisWriteProvider().from(
                makeCfg("WriteToRedis", "host: \"$host\"\nport: $port\nmode: set\n", withErrorHandling = true)
            )
        )
        PAssert.thatSingleton(out.get(Tags.ERROR_OUTPUT).apply(Count.globally())).isEqualTo(1L)
        p.run().waitUntilFinish()
        server.stop()
    }

    @Test
    fun `a row with a null value goes to the dead letter`() {
        // Redis does not allow a null key/value: the write side treats a null value as this row's own
        // problem and routes it to the dead letter via error_handling, instead of silently writing an
        // empty string
        val (server, port) = startServer()
        val host = "localhost"
        val schema = Schema.builder().addStringField("key")
            .addNullableField("value", Schema.FieldType.STRING).build()
        val rows = listOf(Row.withSchema(schema).addValue("k1").addValue(null).build())

        val p = Pipeline.create()
        val input = p.apply(Create.of(rows).withRowSchema(schema))
        val out = PCollectionRowTuple.of(Tags.MAIN_INPUT, input).apply(
            RedisWriteProvider().from(
                makeCfg("WriteToRedis", "host: \"$host\"\nport: $port\nmode: set\n", withErrorHandling = true)
            )
        )
        PAssert.thatSingleton(out.get(Tags.ERROR_OUTPUT).apply(Count.globally())).isEqualTo(1L)
        p.run().waitUntilFinish()
        server.stop()
    }
}
