package com.neighborhood.aka.laplace.hackathon.watermark;

public final class WatermarkAlignAck implements WatermarkAlignEvent {

    private int subtask;
    private Long localTs;

    public WatermarkAlignAck() {}

    public WatermarkAlignAck(int subtask, Long localTs) {
        this.subtask = subtask;
        this.localTs = localTs;
    }

    public int getSubtask() {
        return subtask;
    }

    public WatermarkAlignAck setSubtask(int subtask) {
        this.subtask = subtask;
        return this;
    }

    public Long getLocalTs() {
        return localTs;
    }

    public WatermarkAlignAck setLocalTs(Long localTs) {
        this.localTs = localTs;
        return this;
    }
}
