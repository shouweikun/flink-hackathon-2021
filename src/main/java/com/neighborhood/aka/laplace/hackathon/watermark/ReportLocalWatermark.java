package com.neighborhood.aka.laplace.hackathon.watermark;

public final class ReportLocalWatermark implements WatermarkAlignEvent {

    private int subtask;
    private Long localTs;

    public ReportLocalWatermark() {}

    public ReportLocalWatermark(int subtask, Long localTs) {
        this.subtask = subtask;
        this.localTs = localTs;
    }

    public int getSubtask() {
        return subtask;
    }

    public ReportLocalWatermark setSubtask(int subtask) {
        this.subtask = subtask;
        return this;
    }

    public Long getLocalTs() {
        return localTs;
    }

    public ReportLocalWatermark setLocalTs(Long localTs) {
        this.localTs = localTs;
        return this;
    }
}
