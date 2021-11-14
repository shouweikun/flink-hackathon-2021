package com.neighborhood.aka.laplace.hackathon.version;

import java.io.Serializable;

public final class Versioned implements Comparable<Versioned>, Serializable {

    private long generatedTs;

    private long unifiedVersion;

    public Versioned() {
    }

    public long getGeneratedTs() {
        return generatedTs;
    }

    public Versioned setGeneratedTs(long generatedTs) {
        this.generatedTs = generatedTs;
        return this;
    }

    public long getUnifiedVersion() {
        return unifiedVersion;
    }

    public Versioned setUnifiedVersion(long unifiedVersion) {
        this.unifiedVersion = unifiedVersion;
        return this;
    }

    @Override
    public int compareTo(Versioned o) {
        return Long.compare(this.unifiedVersion, o.unifiedVersion);
    }
}
