package me.jayer.hdata.core.spi

import org.apache.beam.sdk.schemas.transforms.SchemaTransform
import org.apache.beam.sdk.values.PBegin
import org.apache.beam.sdk.values.PCollection
import org.apache.beam.sdk.values.PCollectionRowTuple
import org.apache.beam.sdk.values.Row

/**
 * HData 中所有 transform 的统一形态：`PCollectionRowTuple -> PCollectionRowTuple`。
 *
 * 直接继承 Beam 的 [SchemaTransform]，因此：
 * - source / transform / sink 不再是三种互不相通的类型，而是同一种节点的不同输入输出基数，
 *   DAG 构建器只需要处理一种契约；
 * - 每个节点天然支持多输入（join / flatten）与多输出（主输出 + 死信）；
 * - HData 连接器同时也是合法的 Beam `SchemaTransform`，可以被原生 Beam Java pipeline 直接复用。
 *
 * 大多数连接器不需要直接实现本类，用 [RowSource] / [RowTransform] / [RowSink] 更省事。
 *
 * @author wuya
 * @date 2022-08-30
 */
abstract class HDataTransform : SchemaTransform() {
    companion object {
        private const val serialVersionUID: Long = 1
    }
}

/**
 * 零输入、单输出的读取端。
 */
abstract class RowSource : HDataTransform() {

    protected abstract fun read(begin: PBegin): PCollection<Row>

    final override fun expand(input: PCollectionRowTuple): PCollectionRowTuple =
        PCollectionRowTuple.of(Tags.MAIN_OUTPUT, read(PBegin.`in`(input.pipeline)))

    companion object {
        private const val serialVersionUID: Long = 1
    }
}

/**
 * 单输入、单输出的处理端。
 */
abstract class RowTransform : HDataTransform() {

    protected abstract fun transform(input: PCollection<Row>): PCollection<Row>

    final override fun expand(input: PCollectionRowTuple): PCollectionRowTuple =
        PCollectionRowTuple.of(Tags.MAIN_OUTPUT, transform(input.singlePCollection))

    companion object {
        private const val serialVersionUID: Long = 1
    }
}

/**
 * 单输入、无主输出的写入端，可选地吐出死信流。
 */
abstract class RowSink : HDataTransform() {

    /** @return 写失败的记录（schema 见 [me.jayer.hdata.core.error.ErrorSchemas]），不支持死信时返回 null。 */
    protected abstract fun write(input: PCollection<Row>): PCollection<Row>?

    final override fun expand(input: PCollectionRowTuple): PCollectionRowTuple {
        val errors = write(input.singlePCollection)
        return if (errors == null) {
            PCollectionRowTuple.empty(input.pipeline)
        } else {
            PCollectionRowTuple.of(Tags.ERROR_OUTPUT, errors)
        }
    }

    companion object {
        private const val serialVersionUID: Long = 1
    }
}
