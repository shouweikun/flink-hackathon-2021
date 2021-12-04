package com.neighborhood.aka.laplace.hackathon.watermark;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.operators.coordination.OperatorCoordinator;
import org.apache.flink.runtime.operators.coordination.OperatorEventDispatcher;
import org.apache.flink.streaming.api.operators.*;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeServiceAware;

import com.neighborhood.aka.lapalce.hackathon.watermark.AlignedTimestampsAndWatermarksOperatorCoordinatorProvider;

public class AlignedTimestampsAndWatermarksOperatorFactory<IN, OUT>
        extends AbstractStreamOperatorFactory<OUT>
        implements OneInputStreamOperatorFactory<IN, OUT>,
                CoordinatedOperatorFactory<OUT>,
                ProcessingTimeServiceAware {

    private final WatermarkStrategy<OUT> watermarkStrategy;

    public AlignedTimestampsAndWatermarksOperatorFactory(WatermarkStrategy<OUT> watermarkStrategy) {
        this.watermarkStrategy = watermarkStrategy;
    }

    @Override
    public OperatorCoordinator.Provider getCoordinatorProvider(String s, OperatorID operatorID) {
        return new AlignedTimestampsAndWatermarksOperatorCoordinatorProvider(operatorID);
    }

    @Override
    public <T extends StreamOperator<OUT>> T createStreamOperator(
            StreamOperatorParameters<OUT> streamOperatorParameters) {
        AlignedTimestampsAndWatermarksOperator<OUT> operator =
                new AlignedTimestampsAndWatermarksOperator<>(watermarkStrategy, true);

        final OperatorID operatorId = streamOperatorParameters.getStreamConfig().getOperatorID();
        final OperatorEventDispatcher eventDispatcher =
                streamOperatorParameters.getOperatorEventDispatcher();

        if (operator instanceof AbstractStreamOperator) {
            ((AbstractStreamOperator) operator).setProcessingTimeService(processingTimeService);
        }
        if (operator instanceof SetupableStreamOperator) {
            ((SetupableStreamOperator) operator)
                    .setup(
                            streamOperatorParameters.getContainingTask(),
                            streamOperatorParameters.getStreamConfig(),
                            streamOperatorParameters.getOutput());
        }

        streamOperatorParameters.getOperatorEventDispatcher().registerEventHandler(operatorId, operator);

        operator.setOperatorEventGateway(eventDispatcher.getOperatorEventGateway(operatorId));
        return (T) operator;
    }

    @Override
    public Class<? extends StreamOperator> getStreamOperatorClass(ClassLoader classLoader) {
        return AlignedTimestampsAndWatermarksOperator.class;
    }
}
