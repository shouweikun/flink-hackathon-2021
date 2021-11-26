package com.neighborhood.aka.laplace.hackathon.watermark;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.operators.coordination.OperatorCoordinator;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;
import org.apache.flink.runtime.util.ExecutorThreadFactory;

import org.apache.flink.shaded.guava18.com.google.common.collect.ImmutableList;

import javax.annotation.Nullable;

import java.io.*;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.stream.Collectors;

public class AlignedTimestampsAndWatermarksOperatorCoordinator
        implements OperatorCoordinator, WatermarkAlignMember {

    static class RuntimeContext implements Serializable {
        final Map<Integer, Long> subtaskIdAndLocalWatermark;
        final Map<Integer, Boolean> ackedSubtask;

        public RuntimeContext() {
            this.subtaskIdAndLocalWatermark = new HashMap<>();
            ;
            this.ackedSubtask = new ConcurrentHashMap<>();
        }
    }

    private final int parallelism;
    private final OperatorID operatorID;

    private transient ExecutorService executorService;
    private transient OperatorCoordinator.SubtaskGateway[] subtaskGateways;
    private transient RuntimeContext context;

    public AlignedTimestampsAndWatermarksOperatorCoordinator(
            int parallelism, OperatorID operatorID) {
        this.parallelism = parallelism;
        this.operatorID = operatorID;
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
            subtaskGateways[subTaskId].sendEvent(new ReportLocalWatermarkAck(getGlobalWatermark()));
        } else if (operatorEvent instanceof WatermarkAlignAck) {
            WatermarkAlignAck event = (WatermarkAlignAck) operatorEvent;
            putLocalWatermarkAndUpdateToAlign(event.getSubtask(), event.getLocalTs());
            addAckedSubtask(event.getSubtask());
        }
    }

    @Override
    public void checkpointCoordinator(long checkpointId, CompletableFuture<byte[]> result)
            throws Exception {

        CompletableFuture<List<SubtaskGateway>> sendResult = new CompletableFuture<>();

        final Long globalWatermark = getGlobalWatermark();
        final Map<Integer, Boolean> ackedSubtask = context.ackedSubtask;
        final ImmutableList<SubtaskGateway> gatewayList = ImmutableList.copyOf(subtaskGateways);
        final RuntimeContext runtimeContext = this.context;

        sendResult.handleAsync(
                (list, ex) -> {
                    List<CompletableFuture<Acknowledge>> ackList =
                            list.stream()
                                    .map(
                                            gateway ->
                                                    gateway.sendEvent(
                                                            new WatermarkAlignRequest(
                                                                    globalWatermark)))
                                    .collect(Collectors.toList());

                    try {
                        for (int index = 0; index < list.size(); index++) {
                            ackList.get(index).get();
                            int count = 0;
                            while (true) {
                                if (ackedSubtask.containsKey(index)) {
                                    break;
                                } else {
                                    if (count > 30) {
                                        result.completeExceptionally(new TimeoutException());
                                        return null;
                                    }
                                    count++;
                                    Thread.sleep(200);
                                }
                            }
                        }
                        runtimeContext.ackedSubtask.clear();
                        ByteArrayOutputStream baos = new ByteArrayOutputStream();
                        ObjectOutputStream oos = new ObjectOutputStream(baos);
                        oos.writeObject(runtimeContext);
                        result.complete(baos.toByteArray());
                        return null;
                    } catch (Exception e) {
                        result.completeExceptionally(e);
                        return null;
                    }
                },
                executorService);

        this.checkpointCoordinatorOnAlignment(checkpointId);
        sendResult.complete(gatewayList);
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) {
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
    }

    @Override
    public void subtaskFailed(int i, @Nullable Throwable throwable) {}

    @Override
    public void subtaskReset(int i, long l) {}

    @Override
    public void subtaskReady(int subTaskId, SubtaskGateway subtaskGateway) {
        subtaskGateways[subTaskId] = subtaskGateway;
    }

    private void putLocalWatermarkAndUpdateToAlign(Integer subtaskId, Long localWatermark) {
        Map<Integer, Long> subtaskIdAndLocalWatermark = this.context.subtaskIdAndLocalWatermark;
        if (!subtaskIdAndLocalWatermark.containsKey(subtaskId)) {
            subtaskIdAndLocalWatermark.put(subtaskId, localWatermark);
        } else if (subtaskIdAndLocalWatermark.get(subtaskId) < localWatermark) {
            subtaskIdAndLocalWatermark.put(subtaskId, localWatermark);
        }

        if (subtaskIdAndLocalWatermark.size() == parallelism) {
            Long operatorWatermarkTs =
                    subtaskIdAndLocalWatermark.values().stream().min(Long::compare).orElse(null);
            putOperatorTs(operatorID, operatorWatermarkTs);
        }
    }

    Long getGlobalWatermark() {
        return this.getGlobalAlignedWatermarkTs();
    }

    private void addAckedSubtask(Integer subtaskId) {
        this.context.ackedSubtask.put(subtaskId, true);
    }

    @VisibleForTesting
    public RuntimeContext getRuntimeContext() {
        return this.context;
    }
}
