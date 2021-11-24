package com.neighborhood.aka.laplace.hackathon.watermark;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.operators.coordination.OperatorCoordinator;
import org.apache.flink.runtime.operators.coordination.OperatorEventDispatcher;
import org.apache.flink.streaming.api.operators.AbstractStreamOperatorFactory;
import org.apache.flink.streaming.api.operators.CoordinatedOperatorFactory;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperatorParameters;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeServiceAware;

public class AlignedTimestampsAndWatermarksOperatorFactory<OUT>
        extends AbstractStreamOperatorFactory<OUT>
        implements CoordinatedOperatorFactory<OUT>, ProcessingTimeServiceAware {

    private final WatermarkStrategy<OUT> watermarkStrategy;

    public AlignedTimestampsAndWatermarksOperatorFactory(WatermarkStrategy<OUT> watermarkStrategy) {
        this.watermarkStrategy = watermarkStrategy;
    }

    @Override
    public OperatorCoordinator.Provider getCoordinatorProvider(String s, OperatorID operatorID) {
        return null;
    }

    @Override
    public <T extends StreamOperator<OUT>> T createStreamOperator(
            StreamOperatorParameters<OUT> streamOperatorParameters) {
        AlignedTimestampsAndWatermarksOperator<OUT> operator =
                new AlignedTimestampsAndWatermarksOperator<>(watermarkStrategy, true);

        final OperatorID operatorId = streamOperatorParameters.getStreamConfig().getOperatorID();
        final OperatorEventDispatcher eventDispatcher =
                streamOperatorParameters.getOperatorEventDispatcher();

        operator.setOperatorEventGateway(eventDispatcher.getOperatorEventGateway(operatorId));
        return (T) operator;
    }

    @Override
    public Class<? extends StreamOperator> getStreamOperatorClass(ClassLoader classLoader) {
        return AlignedTimestampsAndWatermarksOperator.class;
    }
}
