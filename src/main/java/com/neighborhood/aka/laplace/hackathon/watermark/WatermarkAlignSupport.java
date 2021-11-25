package com.neighborhood.aka.laplace.hackathon.watermark;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.runtime.jobgraph.OperatorID;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * {@link WatermarkAlignSupport#checkpointCoordinator(long)} and {@link
 * WatermarkAlignSupport#notifyCheckpointComplete(long)} follows the "Checkpoint Subsuming
 * Contract".
 *
 * <p>see @{@link org.apache.flink.api.common.state.CheckpointListener}'s class-level doc.
 */
class WatermarkAlignSupport {

    public interface Timestamp {

        Long getTs();

        Timestamp EMPTY = EmptyTimestamp.INSTANCE;

        static Timestamp fromTs(Long ts) {
            if (ts == null) {
                return Timestamp.EMPTY;
            } else {
                return new ConTimestamp(ts);
            }
        }

        static Long toTs(Timestamp ts) {
            return ts.getTs();
        }
    }

    static final class ConTimestamp implements Timestamp {
        private final long ts;

        public ConTimestamp(long ts) {
            this.ts = ts;
        }

        @Override
        public Long getTs() {
            return ts;
        }
    }

    static final class EmptyTimestamp implements Timestamp {
        public static EmptyTimestamp INSTANCE = new EmptyTimestamp();

        private EmptyTimestamp() {}

        @Override
        public Long getTs() {
            return null;
        }
    }

    private static class CheckpointIdAndAlignTs {
        final Long alignTs;
        final long checkpointId;

        public CheckpointIdAndAlignTs(Long alignTs, long checkpointId) {
            this.alignTs = alignTs;
            this.checkpointId = checkpointId;
        }
    }

    private WatermarkAlignSupport() {}

    private static CheckpointIdAndAlignTs currentCheckpointIdAndAlignTs = null;

    private static Map<OperatorID, Timestamp> tsMap = new ConcurrentHashMap<>();

    static void registerOperator(OperatorID operatorID) {
        tsMap.putIfAbsent(operatorID, Timestamp.fromTs(null));
    }

    static void putOperatorTs(OperatorID operatorID, Long ts) {
        tsMap.put(operatorID, Timestamp.fromTs(ts));
    }

    static synchronized void checkpointCoordinator(long checkpointId) {
        if (currentCheckpointIdAndAlignTs == null
                || currentCheckpointIdAndAlignTs.checkpointId < checkpointId) {
            currentCheckpointIdAndAlignTs = null;
            currentCheckpointIdAndAlignTs = new CheckpointIdAndAlignTs(getGlobalTs(), checkpointId);
        }
    }

    static synchronized void notifyCheckpointComplete(long checkpointId) {
        if (currentCheckpointIdAndAlignTs != null
                && currentCheckpointIdAndAlignTs.checkpointId <= checkpointId) {
            currentCheckpointIdAndAlignTs = null;
        }
    }

    static Long getGlobalTs() {
        Long globalTsInternal = getGlobalTsInternal();
        return globalTsInternal == null ? null : globalTsInternal - 1;
    }

    static Long getGlobalTsInternal() {
        final CheckpointIdAndAlignTs currentCheckpointIdAndAlignTs =
                WatermarkAlignSupport.currentCheckpointIdAndAlignTs;
        if (currentCheckpointIdAndAlignTs != null) {
            return currentCheckpointIdAndAlignTs.alignTs;
        } else {
            Long re = null;
            Set<Map.Entry<OperatorID, Timestamp>> entries = tsMap.entrySet();
            for (Map.Entry<OperatorID, Timestamp> entry : entries) {
                Long currValue = Timestamp.toTs(entry.getValue());
                if (currValue == null) {
                    return null;
                } else {
                    re = re == null ? currValue : Math.min(re, currValue);
                }
            }
            return re;
        }
    }

    @VisibleForTesting
    public static Map<OperatorID, Timestamp> getTsMap() {
        return tsMap;
    }

    @VisibleForTesting
    public static CheckpointIdAndAlignTs getCurrentCheckpointIdAndAlignTs() {
        return currentCheckpointIdAndAlignTs;
    }
}
