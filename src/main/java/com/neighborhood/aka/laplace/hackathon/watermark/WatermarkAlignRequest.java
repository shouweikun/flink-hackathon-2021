package com.neighborhood.aka.laplace.hackathon.watermark;

public final class WatermarkAlignRequest implements WatermarkAlignEvent {

    private Long globalTs;

    private final long checkpointId;

    public Long getGlobalTs() {
        return globalTs;
    }

    public WatermarkAlignRequest setGlobalTs(long globalTs) {
        this.globalTs = globalTs;
        return this;
    }

    public WatermarkAlignRequest setGlobalTs(Long globalTs) {
        this.globalTs = globalTs;
        return this;
    }

    public long getCheckpointId() {
        return checkpointId;
    }

    public WatermarkAlignRequest(Long globalTs, long checkpointId) {
        this.globalTs = globalTs;
        this.checkpointId = checkpointId;
    }
}
