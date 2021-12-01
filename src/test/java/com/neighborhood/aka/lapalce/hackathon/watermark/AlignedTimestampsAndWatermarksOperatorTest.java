package com.neighborhood.aka.lapalce.hackathon.watermark;

import org.apache.flink.api.common.eventtime.TimestampAssigner;
import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkOutput;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;
import org.apache.flink.runtime.operators.coordination.OperatorEventGateway;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;

import com.neighborhood.aka.laplace.hackathon.watermark.AlignedTimestampsAndWatermarksOperator;
import com.neighborhood.aka.laplace.hackathon.watermark.ReportLocalWatermarkAck;
import org.junit.Test;

import java.io.Serializable;
import java.util.Optional;

import static org.apache.flink.streaming.util.StreamRecordMatchers.streamRecord;
import static org.apache.flink.streaming.util.WatermarkMatchers.legacyWatermark;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.*;

public class AlignedTimestampsAndWatermarksOperatorTest {

    private static class MockOperatorEventGateway implements OperatorEventGateway {

        private final AlignedTimestampsAndWatermarksOperator operator;

        public MockOperatorEventGateway(AlignedTimestampsAndWatermarksOperator operator) {
            this.operator = operator;
        }

        @Override
        public void sendEventToCoordinator(OperatorEvent operatorEvent) {}
    }

    private static final long AUTO_WATERMARK_INTERVAL = 50L;

    @Test
    public void inputWatermarksAreNotForwarded() throws Exception {
        OneInputStreamOperatorTestHarness<Long, Long> testHarness =
                createTestHarness(
                        WatermarkStrategy.forGenerator((ctx) -> new PeriodicWatermarkGenerator())
                                .withTimestampAssigner((ctx) -> new LongExtractor()));

        testHarness.processWatermark(createLegacyWatermark(42L));
        testHarness.setProcessingTime(AUTO_WATERMARK_INTERVAL);

        assertThat(testHarness.getOutput(), empty());
    }

    @Test
    public void longMaxInputWatermarkIsForwarded() throws Exception {
        OneInputStreamOperatorTestHarness<Long, Long> testHarness =
                createTestHarness(
                        WatermarkStrategy.forGenerator((ctx) -> new PeriodicWatermarkGenerator())
                                .withTimestampAssigner((ctx) -> new LongExtractor()));

        testHarness.processWatermark(createLegacyWatermark(Long.MAX_VALUE));

        assertEquals(pollNextLegacyWatermark(testHarness), null);

        syncCurrLocalTsToGlobal((AlignedTimestampsAndWatermarksOperator) testHarness.getOperator());
        testHarness.processWatermark(createLegacyWatermark(Long.MAX_VALUE));

        assertThat(pollNextLegacyWatermark(testHarness), is(legacyWatermark(Long.MAX_VALUE)));
    }

    @Test
    public void periodicWatermarksEmitOnPeriodicEmitStreamMode() throws Exception {
        OneInputStreamOperatorTestHarness<Long, Long> testHarness =
                createTestHarness(
                        WatermarkStrategy.forGenerator((ctx) -> new PeriodicWatermarkGenerator())
                                .withTimestampAssigner((ctx) -> new LongExtractor()));

        testHarness.processElement(new StreamRecord<>(2L, 1));
        testHarness.setProcessingTime(AUTO_WATERMARK_INTERVAL);

        assertThat(pollNextStreamRecord(testHarness), streamRecord(2L, 2L));
        syncCurrLocalTsToGlobal((AlignedTimestampsAndWatermarksOperator) testHarness.getOperator());

        testHarness.processElement(new StreamRecord<>(4L, 1));
        testHarness.setProcessingTime(AUTO_WATERMARK_INTERVAL * 2);

        assertThat(pollNextStreamRecord(testHarness), streamRecord(4L, 4L));
        assertThat(pollNextLegacyWatermark(testHarness), is(legacyWatermark(1L)));
    }

    @Test
    public void periodicWatermarksBatchMode() throws Exception {
        OneInputStreamOperatorTestHarness<Long, Long> testHarness =
                createBatchHarness(
                        WatermarkStrategy.forGenerator((ctx) -> new PeriodicWatermarkGenerator())
                                .withTimestampAssigner((ctx) -> new LongExtractor()));

        testHarness.processElement(new StreamRecord<>(2L, 1));
        testHarness.setProcessingTime(AUTO_WATERMARK_INTERVAL);

        assertThat(pollNextStreamRecord(testHarness), streamRecord(2L, 2L));
        assertNull(pollNextLegacyWatermark(testHarness));

        testHarness.processElement(new StreamRecord<>(4L, 1));
        testHarness.setProcessingTime(AUTO_WATERMARK_INTERVAL * 2);

        assertThat(pollNextStreamRecord(testHarness), streamRecord(4L, 4L));
        assertNull(pollNextLegacyWatermark(testHarness));
    }

    @Test
    public void periodicWatermarksOnlyEmitOnPeriodicEmitStreamMode() throws Exception {
        OneInputStreamOperatorTestHarness<Long, Long> testHarness =
                createTestHarness(
                        WatermarkStrategy.forGenerator((ctx) -> new PeriodicWatermarkGenerator())
                                .withTimestampAssigner((ctx) -> new LongExtractor()));

        testHarness.processElement(new StreamRecord<>(2L, 1));

        assertThat(pollNextStreamRecord(testHarness), streamRecord(2L, 2L));
        assertThat(testHarness.getOutput(), empty());
    }

    @Test
    public void punctuatedWatermarksEmitImmediatelyStreamMode() throws Exception {
        OneInputStreamOperatorTestHarness<Tuple2<Boolean, Long>, Tuple2<Boolean, Long>>
                testHarness =
                        createTestHarness(
                                WatermarkStrategy.forGenerator(
                                                (ctx) -> new PunctuatedWatermarkGenerator())
                                        .withTimestampAssigner((ctx) -> new TupleExtractor()));

        testHarness.processElement(new StreamRecord<>(new Tuple2<>(true, 2L), 1));
        pollNextStreamRecord(testHarness);
        syncCurrLocalTsToGlobal((AlignedTimestampsAndWatermarksOperator) testHarness.getOperator());
        testHarness.processElement(new StreamRecord<>(new Tuple2<>(true, 2L), 1));

        assertThat(pollNextStreamRecord(testHarness), streamRecord(new Tuple2<>(true, 2L), 2L));
        assertThat(pollNextLegacyWatermark(testHarness), is(legacyWatermark(2L)));

        testHarness.processElement(new StreamRecord<>(new Tuple2<>(true, 4L), 1));
        pollNextStreamRecord(testHarness);
        syncCurrLocalTsToGlobal((AlignedTimestampsAndWatermarksOperator) testHarness.getOperator());
        testHarness.processElement(new StreamRecord<>(new Tuple2<>(true, 4L), 1));

        assertThat(pollNextStreamRecord(testHarness), streamRecord(new Tuple2<>(true, 4L), 4L));
        assertThat(pollNextLegacyWatermark(testHarness), is(legacyWatermark(4L)));
    }

    @Test
    public void punctuatedWatermarksBatchMode() throws Exception {
        OneInputStreamOperatorTestHarness<Tuple2<Boolean, Long>, Tuple2<Boolean, Long>>
                testHarness =
                        createBatchHarness(
                                WatermarkStrategy.forGenerator(
                                                (ctx) -> new PunctuatedWatermarkGenerator())
                                        .withTimestampAssigner((ctx) -> new TupleExtractor()));

        testHarness.processElement(new StreamRecord<>(new Tuple2<>(true, 2L), 1));

        assertThat(pollNextStreamRecord(testHarness), streamRecord(new Tuple2<>(true, 2L), 2L));
        assertNull(pollNextLegacyWatermark(testHarness));

        testHarness.processElement(new StreamRecord<>(new Tuple2<>(true, 4L), 1));

        assertThat(pollNextStreamRecord(testHarness), streamRecord(new Tuple2<>(true, 4L), 4L));
        assertNull(pollNextLegacyWatermark(testHarness));
    }
    /** Negative timestamps also must be correctly forwarded. */
    @Test
    public void testNegativeTimestamps() throws Exception {

        OneInputStreamOperatorTestHarness<Long, Long> testHarness =
                createTestHarness(
                        WatermarkStrategy.forGenerator((ctx) -> new NeverWatermarkGenerator())
                                .withTimestampAssigner((ctx) -> new LongExtractor()));

        long[] values = {Long.MIN_VALUE, -1L, 0L, 1L, 2L, 3L, Long.MAX_VALUE};

        for (long value : values) {
            testHarness.processElement(new StreamRecord<>(value));
        }

        for (long value : values) {
            assertThat(pollNextStreamRecord(testHarness).getTimestamp(), is(value));
        }
    }

    private static <T> OneInputStreamOperatorTestHarness<T, T> createTestHarness(
            WatermarkStrategy<T> watermarkStrategy) throws Exception {

        final AlignedTimestampsAndWatermarksOperator<T> operator =
                new AlignedTimestampsAndWatermarksOperator<>(watermarkStrategy, true);

        operator.setOperatorEventGateway(new MockOperatorEventGateway(operator));

        OneInputStreamOperatorTestHarness<T, T> testHarness =
                new OneInputStreamOperatorTestHarness<>(operator);

        testHarness.getExecutionConfig().setAutoWatermarkInterval(AUTO_WATERMARK_INTERVAL);

        testHarness.open();

        return testHarness;
    }

    private static void syncCurrLocalTsToGlobal(AlignedTimestampsAndWatermarksOperator operator) {
        Long ts =
                Optional.ofNullable(operator.getCurrentLocalWatermark())
                        .flatMap(wm -> Optional.ofNullable(wm.getTimestamp()))
                        .orElse(null);
        operator.handleOperatorEvent(new ReportLocalWatermarkAck(ts));
    }

    private static <T> OneInputStreamOperatorTestHarness<T, T> createBatchHarness(
            WatermarkStrategy<T> watermarkStrategy) throws Exception {

        final AlignedTimestampsAndWatermarksOperator<T> operator =
                new AlignedTimestampsAndWatermarksOperator<>(watermarkStrategy, false);

        OneInputStreamOperatorTestHarness<T, T> testHarness =
                new OneInputStreamOperatorTestHarness<>(operator);

        testHarness.open();

        return testHarness;
    }

    @SuppressWarnings("unchecked")
    private static <T> StreamRecord<T> pollNextStreamRecord(
            OneInputStreamOperatorTestHarness<?, T> testHarness) {
        return (StreamRecord<T>) testHarness.getOutput().poll();
    }

    private static org.apache.flink.streaming.api.watermark.Watermark pollNextLegacyWatermark(
            OneInputStreamOperatorTestHarness<?, ?> testHarness) {
        return (org.apache.flink.streaming.api.watermark.Watermark) testHarness.getOutput().poll();
    }

    private static org.apache.flink.streaming.api.watermark.Watermark createLegacyWatermark(
            long timestamp) {
        return new org.apache.flink.streaming.api.watermark.Watermark(timestamp);
    }

    private static class LongExtractor implements TimestampAssigner<Long> {
        @Override
        public long extractTimestamp(Long element, long recordTimestamp) {
            return element;
        }
    }

    private static class TupleExtractor implements TimestampAssigner<Tuple2<Boolean, Long>> {
        @Override
        public long extractTimestamp(Tuple2<Boolean, Long> element, long recordTimestamp) {
            return element.f1;
        }
    }

    /**
     * A {@link WatermarkGenerator} that doesn't enforce the watermark invariant by itself. If a
     * record with a lower timestamp than the previous high timestamp comes in the output watermark
     * regressed.
     */
    private static class PeriodicWatermarkGenerator
            implements WatermarkGenerator<Long>, Serializable {

        private long currentWatermark = Long.MIN_VALUE;

        @Override
        public void onEvent(Long event, long eventTimestamp, WatermarkOutput output) {
            currentWatermark = eventTimestamp;
        }

        @Override
        public void onPeriodicEmit(WatermarkOutput output) {
            long effectiveWatermark =
                    currentWatermark == Long.MIN_VALUE ? Long.MIN_VALUE : currentWatermark - 1;
            output.emitWatermark(new Watermark(effectiveWatermark));
        }
    }

    /**
     * A {@link WatermarkGenerator} that doesn't enforce the watermark invariant by itself. If a
     * record with a lower timestamp than the previous high timestamp comes in the output watermark
     * regressed.
     */
    private static class PunctuatedWatermarkGenerator
            implements WatermarkGenerator<Tuple2<Boolean, Long>>, Serializable {
        @Override
        public void onEvent(
                Tuple2<Boolean, Long> event, long eventTimestamp, WatermarkOutput output) {
            if (event.f0) {
                output.emitWatermark(new Watermark(event.f1));
            }
        }

        @Override
        public void onPeriodicEmit(WatermarkOutput output) {}
    }

    private static class NeverWatermarkGenerator implements WatermarkGenerator<Long>, Serializable {

        @Override
        public void onEvent(Long event, long eventTimestamp, WatermarkOutput output) {}

        @Override
        public void onPeriodicEmit(WatermarkOutput output) {}
    }
}
