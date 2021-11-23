package com.neighborhood.aka.laplace.hackathon.watermark;

import org.apache.flink.runtime.jobgraph.OperatorID;

interface WatermarkAlignMember {

    default void putOperatorTs(OperatorID operatorID, Long ts) {
        WatermarkAlignSupport.putOperatorTs(operatorID, ts);
    }

    default void registerOperator(OperatorID operatorID) {
        WatermarkAlignSupport.registerOperator(operatorID);
    }

    default Long getGlobalAlignedWatermarkTs() {
        return WatermarkAlignSupport.getGlobalTs();
    }

    default void checkpointCoordinatorOnAlignment(long checkpointId) {
        WatermarkAlignSupport.checkpointCoordinator(checkpointId);
    }

    default void notifyCheckpointCompleteOnAlignment(long checkpointId) {
        WatermarkAlignSupport.notifyCheckpointComplete(checkpointId);
    }
}
