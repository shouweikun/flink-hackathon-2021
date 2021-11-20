package com.neighborhood.aka.laplace.hackathon.watermark;

public final class LocalWatermarkRequestAck implements WatermarkAlignEvent {

    private int subtask;
    private Long localTs;

    public LocalWatermarkRequestAck() {}

    public LocalWatermarkRequestAck(int subtask, Long localTs) {
        this.subtask = subtask;
        this.localTs = localTs;
    }

    public int getSubtask() {
        return subtask;
    }

    public LocalWatermarkRequestAck setSubtask(int subtask) {
        this.subtask = subtask;
        return this;
    }

    public Long getLocalTs() {
        return localTs;
    }

    public LocalWatermarkRequestAck setLocalTs(Long localTs) {
        this.localTs = localTs;
        return this;
    }
}
