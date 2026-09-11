package me.jayer.hdata.core.spec

import java.io.File
import kotlin.test.Test
import kotlin.test.assertTrue

/**
 * Guarantees the examples under examples/ can always be parsed, so the docs do not drift from the code.
 *
 * Only parsing and structural validation, no graph construction: ReadFromJdbc would need to connect
 * to a database to infer its schema at graph-construction time.
 *
 * @author wuya
 * @date 2022-08-30
 */
class ExamplesTest {

    @Test
    fun `every example parses`() {
        val examples = File("../examples").listFiles { file -> file.extension in SpecMappers.supportedExtensions }
            ?.sortedBy { it.name }
            .orEmpty()
        assertTrue(examples.isNotEmpty(), "could not find the examples directory; run the tests from the repo root")

        val variables = mapOf("MYSQL_PASSWORD" to "test")
        for (example in examples) {
            val spec = PipelineSpecLoader.load(example, variables)
            assertTrue(spec.pipeline.children().isNotEmpty(), "${example.name} has no transforms at all")
        }
    }

}
