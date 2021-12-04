/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.neighborhood.aka.laplace.hackathon.watermark;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.eventtime.NoWatermarksGenerator;
import org.apache.flink.api.common.eventtime.TimestampAssigner;
import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkOutput;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;
import org.apache.flink.runtime.operators.coordination.OperatorEventGateway;
import org.apache.flink.runtime.operators.coordination.OperatorEventHandler;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.streamstatus.StreamStatus;
import org.apache.flink.streaming.runtime.streamstatus.StreamStatusMaintainer;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeCallback;
import org.apache.flink.streaming.runtime.tasks.StreamTask;

import java.util.Optional;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A stream operator that may do one or both of the following: extract timestamps from events and
 * generate watermarks.
 *
 * <p>These two responsibilities run in the same operator rather than in two different ones, because
 * the implementation of the timestamp assigner and the watermark generator is frequently in the
 * same class (and should be run in the same instance), even though the separate interfaces support
 * the use of different classes.
 *
 * @param <T> The type of the input elements
 */
public class AlignedTimestampsAndWatermarksOperator<T> extends AbstractStreamOperator<T>
        implements OneInputStreamOperator<T, T>, ProcessingTimeCallback, OperatorEventHandler {

    private static final long serialVersionUID = 1L;

    private final WatermarkStrategy<T> watermarkStrategy;

    /** The timestamp assigner. */
    private transient TimestampAssigner<T> timestampAssigner;

    /** The watermark generator, initialized during runtime. */
    private transient WatermarkGenerator<T> watermarkGenerator;

    /** The watermark output gateway, initialized during runtime. */
    private transient WatermarkOutput wmOutput;

    /** The interval (in milliseconds) for periodic watermark probes. Initialized during runtime. */
    private transient long watermarkInterval;

    private transient Watermark currentLocalWatermark;

    private transient Watermark globalWatermark;

    private transient OperatorEventGateway operatorEventGateway;

    private transient Integer indexOfThisSubtask;

    private transient Boolean reportingLocalWatermarkMessageIsOnTheWay;

    /** Whether to emit intermediate watermarks or only one final watermark at the end of input. */
    private final boolean emitProgressiveWatermarks;

    public AlignedTimestampsAndWatermarksOperator(
            WatermarkStrategy<T> watermarkStrategy, boolean emitProgressiveWatermarks) {
        this.watermarkStrategy = checkNotNull(watermarkStrategy);
        this.emitProgressiveWatermarks = emitProgressiveWatermarks;
        this.chainingStrategy = ChainingStrategy.DEFAULT_CHAINING_STRATEGY;
    }

    @Override
    public void setup(StreamTask<?, ?> containingTask, StreamConfig config, Output<StreamRecord<T>> output) {
        super.setup(containingTask, config, output);
    }

    @Override
    public void open() throws Exception {
        super.open();
        indexOfThisSubtask = getRuntimeContext().getIndexOfThisSubtask();
        timestampAssigner = watermarkStrategy.createTimestampAssigner(this::getMetricGroup);
        watermarkGenerator =
                emitProgressiveWatermarks
                        ? watermarkStrategy.createWatermarkGenerator(this::getMetricGroup)
                        : new NoWatermarksGenerator<>();

        wmOutput =
                new WatermarkOutput() {

                    WatermarkEmitter internalEmitter =
                            new WatermarkEmitter(
                                    output, getContainingTask().getStreamStatusMaintainer());

                    @Override
                    public void emitWatermark(Watermark watermark) {
                        if (currentLocalWatermark == null
                                || (currentLocalWatermark != null
                                        && currentLocalWatermark.getTimestamp()
                                                < watermark.getTimestamp())) {
                            currentLocalWatermark = watermark;
                        }

                        if (globalWatermark != null) {
                            internalEmitter.emitWatermark(globalWatermark);
                        }
                    }

                    @Override
                    public void markIdle() {
                        internalEmitter.markIdle();
                    }
                };

        watermarkInterval = getExecutionConfig().getAutoWatermarkInterval();
        if (watermarkInterval > 0 && emitProgressiveWatermarks) {
            final long now = getProcessingTimeService().getCurrentProcessingTime();
            getProcessingTimeService().registerTimer(now + watermarkInterval, this);
        }
    }

    @Override
    public void handleOperatorEvent(OperatorEvent operatorEvent) {

        if (operatorEvent instanceof WatermarkAlignRequest) {
            WatermarkAlignRequest request = (WatermarkAlignRequest) operatorEvent;

            Optional.ofNullable(request.getGlobalTs()).ifPresent(ts -> updateGlobalWatermark(ts));

            operatorEventGateway.sendEventToCoordinator(
                    new WatermarkAlignAck(
                            indexOfThisSubtask,
                            Optional.ofNullable(currentLocalWatermark)
                                    .map(wm -> wm.getTimestamp())
                                    .orElse(null)));

        } else if (operatorEvent instanceof ReportLocalWatermarkAck) {
            ReportLocalWatermarkAck watermarkAck = (ReportLocalWatermarkAck) operatorEvent;
            Optional.ofNullable(watermarkAck.getGlobalTs())
                    .ifPresent(ts -> updateGlobalWatermark(ts));
            reportingLocalWatermarkMessageIsOnTheWay = false;
        }
    }

    @Override
    public void processElement(final StreamRecord<T> element) throws Exception {
        final T event = element.getValue();
        final long previousTimestamp =
                element.hasTimestamp() ? element.getTimestamp() : Long.MIN_VALUE;
        final long newTimestamp = timestampAssigner.extractTimestamp(event, previousTimestamp);

        element.setTimestamp(newTimestamp);
        output.collect(element);
        watermarkGenerator.onEvent(event, newTimestamp, wmOutput);
    }

    @Override
    public void onProcessingTime(long timestamp) throws Exception {
        watermarkGenerator.onPeriodicEmit(wmOutput);

        if (reportingLocalWatermarkMessageIsOnTheWay == null
                || (!reportingLocalWatermarkMessageIsOnTheWay)) {
            operatorEventGateway.sendEventToCoordinator(
                    new ReportLocalWatermark(
                            indexOfThisSubtask,
                            Optional.ofNullable(currentLocalWatermark)
                                    .map(wm -> wm.getTimestamp())
                                    .orElse(null)));
        }
        final long now = getProcessingTimeService().getCurrentProcessingTime();
        getProcessingTimeService().registerTimer(now + watermarkInterval, this);
    }

    /**
     * Override the base implementation to completely ignore watermarks propagated from upstream,
     * except for the "end of time" watermark.
     */
    @Override
    public void processWatermark(org.apache.flink.streaming.api.watermark.Watermark mark)
            throws Exception {
        // if we receive a Long.MAX_VALUE watermark we forward it since it is used
        // to signal the end of input and to not block watermark progress downstream
        if (mark.getTimestamp() == Long.MAX_VALUE) {
            wmOutput.emitWatermark(Watermark.MAX_WATERMARK);
        }
    }

    @Override
    public void close() throws Exception {
        super.close();
        watermarkGenerator.onPeriodicEmit(wmOutput);
    }

    // ------------------------------------------------------------------------

    /**
     * Implementation of the {@code WatermarkEmitter}, based on the components that are available
     * inside a stream operator.
     */
    public static final class WatermarkEmitter implements WatermarkOutput {

        private final Output<?> output;

        private final StreamStatusMaintainer statusMaintainer;

        private long currentWatermark;

        private boolean idle;

        public WatermarkEmitter(Output<?> output, StreamStatusMaintainer statusMaintainer) {
            this.output = output;
            this.statusMaintainer = statusMaintainer;
            this.currentWatermark = Long.MIN_VALUE;
        }

        @Override
        public void emitWatermark(Watermark watermark) {
            final long ts = watermark.getTimestamp();

            if (ts <= currentWatermark) {
                return;
            }

            currentWatermark = ts;

            if (idle) {
                idle = false;
                statusMaintainer.toggleStreamStatus(StreamStatus.ACTIVE);
            }

            output.emitWatermark(new org.apache.flink.streaming.api.watermark.Watermark(ts));
        }

        @Override
        public void markIdle() {
            idle = true;
            statusMaintainer.toggleStreamStatus(StreamStatus.IDLE);
        }
    }

    public AlignedTimestampsAndWatermarksOperator<T> setOperatorEventGateway(
            OperatorEventGateway operatorEventGateway) {
        this.operatorEventGateway = operatorEventGateway;
        return this;
    }

    private void updateGlobalWatermark(long ts) {
        if (globalWatermark == null) {
            globalWatermark = new Watermark(ts);
        } else if (globalWatermark.getTimestamp() < ts) {
            globalWatermark = new Watermark(ts);
        }
    }

    @VisibleForTesting
    public Watermark getCurrentLocalWatermark() {
        return currentLocalWatermark;
    }

    @Override
    public MetricGroup getMetricGroup() {

        MetricGroup metricGroup = super.getMetricGroup();
        return metricGroup;
    }
}
