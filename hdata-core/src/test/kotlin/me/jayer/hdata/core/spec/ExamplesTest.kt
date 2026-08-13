package me.jayer.hdata.core.spec

import java.io.File
import kotlin.test.Test
import kotlin.test.assertTrue

/**
 * 保证 examples/ 下的示例始终能被解析，防止文档随代码漂移。
 *
 * 只做解析与结构校验，不构图：ReadFromJdbc 在构图阶段要连库推断 schema。
 *
 * @author wuya
 * @date 2022-08-30
 */
class ExamplesTest {

    @Test
    fun `所有示例都能解析`() {
        val examples = File("../examples").listFiles { file -> file.extension in SpecMappers.supportedExtensions }
            ?.sortedBy { it.name }
            .orEmpty()
        assertTrue(examples.isNotEmpty(), "找不到 examples 目录，请在仓库根目录运行测试")

        val variables = mapOf("MYSQL_PASSWORD" to "test")
        for (example in examples) {
            val spec = PipelineSpecLoader.load(example, variables)
            assertTrue(spec.pipeline.children().isNotEmpty(), "${example.name} 没有任何 transform")
        }
    }

}
