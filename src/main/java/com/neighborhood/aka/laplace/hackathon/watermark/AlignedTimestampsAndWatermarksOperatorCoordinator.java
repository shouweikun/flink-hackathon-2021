package com.neighborhood.aka.laplace.hackathon.watermark;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.operators.coordination.OperatorCoordinator;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;
import org.apache.flink.runtime.util.ExecutorThreadFactory;

import org.apache.flink.shaded.guava18.com.google.common.collect.ImmutableList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.*;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.function.Consumer;

public class AlignedTimestampsAndWatermarksOperatorCoordinator
        implements OperatorCoordinator, WatermarkAlignMember {

    static class RuntimeContext implements Serializable {
        final Map<Integer, Long> subtaskIdAndLocalWatermark;

        public RuntimeContext() {
            this.subtaskIdAndLocalWatermark = new HashMap<>();
        }
    }

    private static final Logger logger =
            LoggerFactory.getLogger(AlignedTimestampsAndWatermarksOperatorCoordinator.class);

    private final int parallelism;
    private final OperatorID operatorID;
    private final Consumer<Throwable> failHandler;

    private transient ExecutorService executorService;
    private transient ExecutorService messageDeliveryCheckExecutorService;
    private transient OperatorCoordinator.SubtaskGateway[] subtaskGateways;
    private transient RuntimeContext context;
    @VisibleForTesting public transient volatile CountDownLatch alignCountDownLatch;

    public AlignedTimestampsAndWatermarksOperatorCoordinator(
            int parallelism, OperatorID operatorID, Consumer<Throwable> failHandler) {
        this.parallelism = parallelism;
        this.operatorID = operatorID;
        this.failHandler = failHandler;
    }

    @Override
    public void start() throws Exception {
        registerOperator(operatorID);
        subtaskGateways = new SubtaskGateway[parallelism];
        if (context == null) { // if context is not null, it must start from resetToCheckpoint
            context = new RuntimeContext();
        }
        executorService =
                Executors.newSingleThreadExecutor(
                        new ExecutorThreadFactory(
                                "AlignedTimestampsAndWatermarksOperatorCoordinator-" + operatorID));

        messageDeliveryCheckExecutorService =
                Executors.newSingleThreadExecutor(
                        new ExecutorThreadFactory(
                                "AlignedTimestampsAndWatermarksOperatorCoordinator-messageCheck-"
                                        + operatorID));
    }

    @Override
    public void close() throws Exception {
        if (executorService != null) {
            executorService.shutdown();
        }
    }

    @Override
    public void handleEventFromOperator(int subTaskId, OperatorEvent operatorEvent)
            throws Exception {

        if (operatorEvent instanceof ReportLocalWatermark) {
            ReportLocalWatermark event = (ReportLocalWatermark) operatorEvent;
            putLocalWatermarkAndUpdateToAlign(event.getSubtask(), event.getLocalTs());
            Long globalWatermark = getGlobalWatermark();
            sendEvent(subtaskGateways[subTaskId], new ReportLocalWatermarkAck(globalWatermark));
        } else if (operatorEvent instanceof WatermarkAlignAck) {
            WatermarkAlignAck event = (WatermarkAlignAck) operatorEvent;
            putLocalWatermarkAndUpdateToAlign(event.getSubtask(), event.getLocalTs());
            alignCountDownLatch.countDown();
        }
    }

    @Override
    public void checkpointCoordinator(long checkpointId, CompletableFuture<byte[]> result)
            throws Exception {
        logger.info(operatorID + " processing checkpointCoordinator, checkpointId:" + checkpointId);
        this.checkpointCoordinatorOnAlignment(checkpointId);
        final Long globalWatermark = getGlobalWatermark();
        final ImmutableList<SubtaskGateway> gatewayList = ImmutableList.copyOf(subtaskGateways);
        final RuntimeContext runtimeContext = this.context;
        final CountDownLatch countDownLatch = new CountDownLatch(parallelism);
        this.alignCountDownLatch = countDownLatch;

        final WatermarkAlignRequest event = new WatermarkAlignRequest(globalWatermark);

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(baos);
        oos.writeObject(runtimeContext);
        final byte[] bytes = baos.toByteArray();
        CompletableFuture<List<SubtaskGateway>> sendResult = new CompletableFuture<>();

        sendResult
                .handleAsync(
                        (list, ex) -> {
                            list.stream().forEach(gateway -> sendEvent(gateway, event));
                            return null;
                        },
                        executorService)
                .handleAsync(
                        (r, ex) -> {
                            try {
                                countDownLatch.await(60, TimeUnit.SECONDS);
                                result.complete(bytes);
                                return null;

                            } catch (Exception e) {
                                result.completeExceptionally(e);
                                return null;
                            }
                        },
                        executorService);

        sendResult.complete(gatewayList);
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) {
        logger.info("process notifyCheckpointComplete: " + checkpointId + "operator:" + operatorID);
        this.notifyCheckpointCompleteOnAlignment(checkpointId);
    }

    @Override
    public void resetToCheckpoint(long checkpointId, @Nullable byte[] checkpointData)
            throws Exception {

        if (checkpointData != null) {
            ByteArrayInputStream bais = new ByteArrayInputStream(checkpointData);
            ObjectInputStream ois = new ObjectInputStream(bais);
            context = (RuntimeContext) ois.readObject();
        }

        updateOperatorTs();
    }

    @Override
    public void subtaskFailed(int i, @Nullable Throwable throwable) {}

    @Override
    public void subtaskReset(int i, long l) {}

    @Override
    public void subtaskReady(int subTaskId, SubtaskGateway subtaskGateway) {
        subtaskGateways[subTaskId] = subtaskGateway;
    }

    private void sendEvent(SubtaskGateway gateway, OperatorEvent event) {
        gateway.sendEvent(event)
                .handleAsync(
                        (f, e) -> {
                            if (e != null) {
                                failHandler.accept(e);
                            }
                            return null;
                        },
                        messageDeliveryCheckExecutorService);
    }

    private void putLocalWatermarkAndUpdateToAlign(Integer subtaskId, Long localWatermark) {
        Map<Integer, Long> subtaskIdAndLocalWatermark = this.context.subtaskIdAndLocalWatermark;
        if (!subtaskIdAndLocalWatermark.containsKey(subtaskId)) {
            subtaskIdAndLocalWatermark.put(subtaskId, localWatermark);
        } else if (subtaskIdAndLocalWatermark.get(subtaskId) < localWatermark) {
            subtaskIdAndLocalWatermark.put(subtaskId, localWatermark);
        }
        updateOperatorTs();
    }

    private void updateOperatorTs() {
        putOperatorTs(operatorID, computeOperatorTs());
    }

    @VisibleForTesting
    public Long computeOperatorTs() {
        Map<Integer, Long> subtaskIdAndLocalWatermark = this.context.subtaskIdAndLocalWatermark;
        if (subtaskIdAndLocalWatermark.size() == parallelism) {
            Long operatorWatermarkTs =
                    subtaskIdAndLocalWatermark.values().stream().min(Long::compare).orElse(null);
            return operatorWatermarkTs;
        } else {
            return null;
        }
    }

    Long getGlobalWatermark() {
        return this.getGlobalAlignedWatermarkTs();
    }

    @VisibleForTesting
    public RuntimeContext getRuntimeContext() {
        return this.context;
    }

    @VisibleForTesting
    public OperatorCoordinator.SubtaskGateway[] getSubtaskGateways() {
        return this.subtaskGateways;
    }
}
