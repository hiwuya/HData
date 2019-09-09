package io.jayer.hdata.core;

import org.apache.beam.sdk.io.range.OffsetRange;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.splittabledofn.OffsetRangeTracker;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker;
import org.apache.beam.sdk.values.Row;

/**
 * @author Jayer
 * @date 2019-07-25
 */
public class JdbcReadSplittableRowDoFn extends DoFn<Void, Row> {

    private static final long serialVersionUID = -5385017875361283122L;

    @GetInitialRestriction
    public OffsetRange getInitialRestriction(Void input) {
        return new OffsetRange(1, 100);
    }

    @SplitRestriction
    public void splitRestriction(Void input, OffsetRange range, OutputReceiver<OffsetRange> receiver) {
        for (int i = 0; i < 100; i++) {
            if (i == 0) {
                receiver.output(new OffsetRange(1, 100));
            } else {
                receiver.output(new OffsetRange(i * 100, (i + 1) * 100));
            }
        }
    }

    @ProcessElement
    public void processElement(ProcessContext c, RestrictionTracker<OffsetRange, Long> tracker) {
        Schema schema = Schema.builder().addInt64Field("taskId").addStringField("day").build();
        final OffsetRange range = tracker.currentRestriction();
        for (long i = range.getFrom(); i < range.getTo(); i++) {
            Row row = Row.withSchema(schema).addValues(i, "row-" + Thread.currentThread().getId()).build();
            if (!tracker.tryClaim(i)) {
                return;
            }

            c.output(row);
        }

        tracker.tryClaim(range.getTo());
    }

    @NewTracker
    public OffsetRangeTracker newTracker(OffsetRange range) {
        return new OffsetRangeTracker(range);
    }
}
