package com.neighborhood.aka.laplace.hackathon.version;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Objects;

public final class Versioned implements Comparable<Versioned>, Serializable {

    public static Versioned of(long generatedTs, long[] unifiedVersion, boolean isHeartbeat) {

        Versioned versioned = new Versioned(isHeartbeat);
        versioned.setUnifiedVersion(unifiedVersion);
        versioned.setGeneratedTs(generatedTs);
        return versioned;
    }

    private long generatedTs;

    private long[] unifiedVersion;

    private final boolean isHeartbeat;

    public Versioned() {
        this(false);
    }

    public Versioned(boolean isHeartbeat) {
        this.isHeartbeat = isHeartbeat;
    }

    public long getGeneratedTs() {
        return generatedTs;
    }

    public Versioned setGeneratedTs(long generatedTs) {
        this.generatedTs = generatedTs;
        return this;
    }

    public long[] getUnifiedVersion() {
        return unifiedVersion;
    }

    public Versioned setUnifiedVersion(long[] unifiedVersion) {
        this.unifiedVersion = unifiedVersion;
        return this;
    }

    public boolean isHeartbeat() {
        return isHeartbeat;
    }

    @Override
    public int compareTo(Versioned o) {
        long[] thisArray = this.unifiedVersion;
        long[] otherArray = o.unifiedVersion;
        if (thisArray.length != otherArray.length) {
            throw new RuntimeException();
        }

        for (int i = 0; i < thisArray.length; i++) {
            int compare = Long.compare(thisArray[i], otherArray[i]);
            if (compare == 0) {
                continue;
            }
            return compare;
        }
        return 0;
    }

    @Override
    public String toString() {
        return "Versioned{"
                + "generatedTs="
                + generatedTs
                + ", unifiedVersion="
                + Arrays.asList(unifiedVersion)
                + '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Versioned versioned = (Versioned) o;
        return generatedTs == versioned.generatedTs
                && unifiedVersion == versioned.unifiedVersion
                && isHeartbeat == versioned.isHeartbeat;
    }

    @Override
    public int hashCode() {
        return Objects.hash(generatedTs, unifiedVersion, isHeartbeat);
    }
}
