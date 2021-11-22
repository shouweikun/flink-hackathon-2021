package com.neighborhood.aka.laplace.hackathon.watermark;

public final class ReportLocalWatermarkAck implements WatermarkAlignEvent {

    private Long globalTs;

    public ReportLocalWatermarkAck() {}

    public ReportLocalWatermarkAck(Long globalTs) {
        this.globalTs = globalTs;
    }

    public Long getGlobalTs() {
        return globalTs;
    }

    public ReportLocalWatermarkAck setGlobalTs(Long globalTs) {
        this.globalTs = globalTs;
        return this;
    }
}
