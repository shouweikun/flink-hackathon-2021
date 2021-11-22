package com.neighborhood.aka.laplace.hackathon.watermark;

public final class WatermarkAlignRequest implements WatermarkAlignEvent {

    private Long globalTs;

    public long getGlobalTs() {
        return globalTs;
    }

    public WatermarkAlignRequest setGlobalTs(long globalTs) {
        this.globalTs = globalTs;
        return this;
    }

    public WatermarkAlignRequest(Long globalTs) {
        this.globalTs = globalTs;
    }

    public WatermarkAlignRequest() {}
}
