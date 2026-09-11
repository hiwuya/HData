package me.jayer.hdata.core.spec

import me.jayer.hdata.core.exception.HDataException
import tools.jackson.databind.node.JsonNodeFactory
import kotlin.test.Test
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

/**
 * Structural validation exposed by [TransformSpec] after parsing: type constraints on config / input /
 * output / references.
 *
 * These branches are spread across `configNode()` / `inputRefs()` / `outputRefs()`; a wrong type
 * written in YAML should error out at graph-construction time, and no test case pinned this down
 * before — exactly the kind of "validation declared but never actually exercised" spot that regresses
 * easily.
 */
class TransformSpecTest {

    @Test
    fun `config must be an object`() {
        val spec = TransformSpec(type = "Foo", config = JsonNodeFactory.instance.textNode("not-an-object"))
        val error = assertFailsWith<HDataException> { spec.configNode() }
        assertTrue("config" in error.message!! && "must be an object" in error.message!!, error.message)
    }

    @Test
    fun `an invalid input type errors out`() {
        // input written as a number/boolean scalar — neither a string, an array, nor an object
        val spec = TransformSpec(type = "Foo", input = JsonNodeFactory.instance.numberNode(1))
        val error = assertFailsWith<HDataException> { spec.inputRefs() }
        assertTrue("input" in error.message!! && "must be a string, array or object" in error.message!!, error.message)
    }

    @Test
    fun `an invalid output type errors out`() {
        val spec = TransformSpec(
            type = "composite",
            transforms = emptyList(),
            output = JsonNodeFactory.instance.numberNode(1),
        )
        val error = assertFailsWith<HDataException> { spec.outputRefs() }
        assertTrue("output" in error.message!! && "must be a string or object" in error.message!!, error.message)
    }

    @Test
    fun `a reference inside input must be a string`() {
        // input: {a: 1} -- the value for port name a must be a node-name string; a number is invalid
        val node = JsonNodeFactory.instance.objectNode().put("a", 1)
        val spec = TransformSpec(type = "Foo", input = node)
        val error = assertFailsWith<HDataException> { spec.inputRefs() }
        assertTrue("must be a string" in error.message!!, error.message)
    }
}
