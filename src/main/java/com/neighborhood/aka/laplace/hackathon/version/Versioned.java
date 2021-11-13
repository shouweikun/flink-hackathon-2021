package com.neighborhood.aka.laplace.hackathon.version;

import java.io.Serializable;

public interface Versioned<T extends Versioned<T>> extends Comparable<T>, Serializable {

    boolean isZeroVersion();

    long getGeneratedTs();

    @Override
    default int compareTo(T o) {
        if (this.isZeroVersion()) {
            if (o.isZeroVersion()) {
                return 0;
            } else {
                return -1;
            }
        } else {
            if (o.isZeroVersion()) {
                return 1;
            } else {
                return compareToNonZeroVersionObject(o);
            }
        }
    }

    int compareToNonZeroVersionObject(T o);
}
