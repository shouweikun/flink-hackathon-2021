package com.neighborhood.aka.laplace.hackathon.watermark;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.runtime.jobgraph.OperatorID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
public class WatermarkAlignSupport {

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

    private static final Logger logger = LoggerFactory.getLogger(WatermarkAlignSupport.class);

    private WatermarkAlignSupport() {}

    @VisibleForTesting static CheckpointIdAndAlignTs currentCheckpointIdAndAlignTs = null;

    private static Map<OperatorID, Timestamp> tsMap = new ConcurrentHashMap<>();

    static void registerOperator(OperatorID operatorID) {
        tsMap.putIfAbsent(operatorID, Timestamp.fromTs(null));
    }

    static void putOperatorTs(OperatorID operatorID, Long ts) {
        tsMap.put(operatorID, Timestamp.fromTs(ts));
    }

    static synchronized boolean checkpointCoordinator(long checkpointId) {
        if (currentCheckpointIdAndAlignTs == null
                || currentCheckpointIdAndAlignTs.checkpointId < checkpointId) {
            currentCheckpointIdAndAlignTs = null;
            Long globalTs = getGlobalTsInternal();
            currentCheckpointIdAndAlignTs = new CheckpointIdAndAlignTs(globalTs, checkpointId);
            logger.info(
                    "WarkmarkAlignSupport process checkpointCoordinator:"
                            + checkpointId
                            + "global Internal ts:"
                            + globalTs);
            return true;
        } else {
            return false;
        }
    }

    static synchronized boolean notifyCheckpointComplete(long checkpointId) {
        if (currentCheckpointIdAndAlignTs != null
                && currentCheckpointIdAndAlignTs.checkpointId <= checkpointId) {
            currentCheckpointIdAndAlignTs = null;
            return true;
        } else {
            return false;
        }
    }

    static Long getGlobalTs() {
        Long globalTsInternal = getGlobalTsInternal();
        return globalTsInternal == null
                ? null
                : (globalTsInternal == Long.MIN_VALUE) ? globalTsInternal : globalTsInternal - 1;
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
}
