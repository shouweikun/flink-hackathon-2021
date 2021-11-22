package com.neighborhood.aka.laplace.hackathon.watermark;

import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.operators.coordination.OperatorCoordinator;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;

import javax.annotation.Nullable;

import java.io.*;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

public class AlignedTimestampsAndWatermarksOperatorCoordinator implements OperatorCoordinator {

    static class RuntimeContext implements Serializable {
        Map<Integer, Long> subtaskIdAndLocalWatermark;
        Map<Integer, Boolean> ackedSubtask;

        public RuntimeContext() {
            this.subtaskIdAndLocalWatermark = new HashMap<>();
            ;
            this.ackedSubtask = new ConcurrentHashMap<>();
        }
    }

    private final int parallelism;
    private final OperatorID operatorID;

    private transient OperatorCoordinator.SubtaskGateway[] subtaskGateways;
    private transient RuntimeContext context;

    public AlignedTimestampsAndWatermarksOperatorCoordinator(
            int parallelism, OperatorID operatorID) {
        this.parallelism = parallelism;
        this.operatorID = operatorID;
    }

    @Override
    public void start() throws Exception {
        subtaskGateways = new SubtaskGateway[parallelism];
        context = new RuntimeContext();
    }

    @Override
    public void close() throws Exception {}

    @Override
    public void handleEventFromOperator(int subTaskId, OperatorEvent operatorEvent)
            throws Exception {

        if (operatorEvent instanceof ReportLocalWatermark) {
            ReportLocalWatermark event = (ReportLocalWatermark) operatorEvent;
            putLocalWatermark(event.getSubtask(), event.getLocalTs());
            subtaskGateways[subTaskId].sendEvent(new ReportLocalWatermarkAck(getGlobalWatermark()));
        } else if (operatorEvent instanceof WatermarkAlignAck) {
            WatermarkAlignAck event = (WatermarkAlignAck) operatorEvent;
            putLocalWatermark(event.getSubtask(), event.getLocalTs());
            addAckedSubtask(event.getSubtask());
        }
    }

    @Override
    public void checkpointCoordinator(long checkpointId, CompletableFuture<byte[]> result)
            throws Exception {
        // copy from CollectSinkOperatorCoordinator
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(baos);
        oos.writeObject(context);

        result.complete(baos.toByteArray());
    }

    @Override
    public void notifyCheckpointComplete(long l) {}

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

    private void putLocalWatermark(Integer subtaskId, Long localWatermark) {
        this.context.subtaskIdAndLocalWatermark.put(subtaskId, localWatermark);
    }

    private Long getGlobalWatermark() {
        // todo
        return null;
    }

    private void addAckedSubtask(Integer subtaskId) {
        this.context.ackedSubtask.put(subtaskId, true);
    }
}
