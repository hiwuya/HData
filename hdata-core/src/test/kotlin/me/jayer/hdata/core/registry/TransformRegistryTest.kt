package me.jayer.hdata.core.registry

import me.jayer.hdata.core.exception.HDataException
import me.jayer.hdata.core.spi.Tags
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue

/**
 * @author wuya
 * @date 2022-08-30
 */
class TransformRegistryTest {

    private val registry = TransformRegistry.discover()

    @Test
    fun `内置 transform 全部可被发现`() {
        val builtin = listOf(
            "Create", "MapToFields", "Flatten", "LogForTesting", "StripErrorMetadata", "AssertEqual",
        )
        builtin.forEach { assertNotNull(registry.find(it), "未发现内置 transform: $it") }
    }

    @Test
    fun `测试目录里的 provider 也能通过 ServiceLoader 发现`() {
        assertNotNull(registry.find("TestSink"))
    }

    @Test
    fun `端口声明区分读取端、处理端与写入端`() {
        assertEquals(emptyList(), registry.get("Create").inputCollectionNames())
        assertEquals(listOf(Tags.MAIN_OUTPUT), registry.get("Create").outputCollectionNames())

        assertEquals(listOf(Tags.MAIN_INPUT), registry.get("MapToFields").inputCollectionNames())
        assertEquals(listOf(Tags.MAIN_OUTPUT), registry.get("MapToFields").outputCollectionNames())

        // Flatten 是变元的
        assertEquals(listOf(Tags.ANY), registry.get("Flatten").inputCollectionNames())

        // 写入端没有主输出，只有死信端口
        assertEquals(listOf(Tags.ERROR_OUTPUT), registry.get("TestSink").outputCollectionNames())
    }

    @Test
    fun `find 对未知类型返回 null，get 抛异常`() {
        assertNull(registry.find("NoSuchTransform"))

        val error = assertFailsWith<HDataException> { registry.get("NoSuchTransform") }
        assertTrue("NoSuchTransform" in error.message!!)
        // 报错里要列出已注册类型，但不该把一长串 Beam URN 也倒出来
        assertTrue("Create" in error.message!!)
        assertFalse("beam:schematransform" in error.message!!)
    }

    @Test
    fun `大小写写错时给出候选`() {
        val error = assertFailsWith<HDataException> { registry.get("createx") }
        assertTrue("Create" in error.message!!)
    }

    @Test
    fun `大小写完全匹配的候选也会被建议`() {
        val error = assertFailsWith<HDataException> { registry.get("create") }
        assertTrue("是否想用" in error.message!!)
        assertTrue("Create" in error.message!!)
    }

    @Test
    fun `部分匹配（包含）时给出候选`() {
        val error = assertFailsWith<HDataException> { registry.get("creat") }
        assertTrue("是否想用" in error.message!!)
        assertTrue("Create" in error.message!!)
    }

    @Test
    fun `完全陌生的类型不给出候选`() {
        val error = assertFailsWith<HDataException> { registry.get("zzz999") }
        assertFalse("是否想用" in error.message!!)
    }

    @Test
    fun `classpath 上的 Beam 原生 SchemaTransformProvider 按 URN 注册`() {
        // beam-sdks-java-core 自带若干 SchemaTransformProvider，桥接后 URN 应当以 beam: 开头
        val beamUrns = registry.identifiers.filter { it.startsWith("beam:") }
        beamUrns.forEach { urn ->
            assertTrue(registry.find(urn) is BeamSchemaTransformAdapter, "$urn 应当由适配器提供")
        }
    }

}
