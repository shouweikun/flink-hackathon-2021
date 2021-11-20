package com.neighborhood.aka.laplace.hackathon.watermark;

public final class LocalWatermarkRequest implements WatermarkAlignEvent {

    private Long globalTs;

    public long getGlobalTs() {
        return globalTs;
    }

    public LocalWatermarkRequest setGlobalTs(long globalTs) {
        this.globalTs = globalTs;
        return this;
    }

    public LocalWatermarkRequest(Long globalTs) {
        this.globalTs = globalTs;
    }

    public LocalWatermarkRequest(long globalTs) {
        this.globalTs = globalTs;
    }
}
