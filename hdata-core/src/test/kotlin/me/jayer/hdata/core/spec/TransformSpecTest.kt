package me.jayer.hdata.core.spec

import me.jayer.hdata.core.exception.HDataException
import tools.jackson.databind.node.JsonNodeFactory
import kotlin.test.Test
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

/**
 * [TransformSpec] 在解析后暴露的结构校验：config / input / output / 引用 的类型约束。
 *
 * 这些分支分散在 `configNode()` / `inputRefs()` / `outputRefs()` 里，靠 YAML 写出错误类型
 * 时就该在构图期报错，此前没有任何用例钉住，属于「声明了校验却没人验证」的易回归点。
 */
class TransformSpecTest {

    @Test
    fun `config 必须是对象`() {
        val spec = TransformSpec(type = "Foo", config = JsonNodeFactory.instance.textNode("not-an-object"))
        val error = assertFailsWith<HDataException> { spec.configNode() }
        assertTrue("config" in error.message!! && "must be an object" in error.message!!, error.message)
    }

    @Test
    fun `input 类型非法报错`() {
        // input 写成数字/布尔这类标量，既不是字符串、数组也不是对象
        val spec = TransformSpec(type = "Foo", input = JsonNodeFactory.instance.numberNode(1))
        val error = assertFailsWith<HDataException> { spec.inputRefs() }
        assertTrue("input" in error.message!! && "must be a string, array or object" in error.message!!, error.message)
    }

    @Test
    fun `output 类型非法报错`() {
        val spec = TransformSpec(
            type = "composite",
            transforms = emptyList(),
            output = JsonNodeFactory.instance.numberNode(1),
        )
        val error = assertFailsWith<HDataException> { spec.outputRefs() }
        assertTrue("output" in error.message!! && "must be a string or object" in error.message!!, error.message)
    }

    @Test
    fun `input 里的引用必须是字符串`() {
        // input: {a: 1} —— 端口名 a 对应的值必须是节点名字符串，写成数字就非法
        val node = JsonNodeFactory.instance.objectNode().put("a", 1)
        val spec = TransformSpec(type = "Foo", input = node)
        val error = assertFailsWith<HDataException> { spec.inputRefs() }
        assertTrue("must be a string" in error.message!!, error.message)
    }
}
