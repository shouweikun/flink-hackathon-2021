package com.neighborhood.aka.lapalce.hackathon.watermark;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.runtime.util.FatalExitExceptionHandler;

import java.util.concurrent.ThreadFactory;

public class CoordinatorExecutorThreadFactory implements ThreadFactory {
    private final String coordinatorThreadName;
    private final ClassLoader cl;
    private final Thread.UncaughtExceptionHandler errorHandler;
    private Thread t;

    public CoordinatorExecutorThreadFactory(
            String coordinatorThreadName, ClassLoader contextClassLoader) {
        this(coordinatorThreadName, contextClassLoader, FatalExitExceptionHandler.INSTANCE);
    }

    @VisibleForTesting
    public CoordinatorExecutorThreadFactory(
            String coordinatorThreadName,
            ClassLoader contextClassLoader,
            Thread.UncaughtExceptionHandler errorHandler) {
        this.coordinatorThreadName = coordinatorThreadName;
        this.cl = contextClassLoader;
        this.errorHandler = errorHandler;
    }

    public synchronized Thread newThread(Runnable r) {
        if (this.t != null) {
            throw new Error(
                    "This indicates that a fatal error has happened and caused the coordinator executor thread to exit. Check the earlier logsto see the root cause of the problem.");
        } else {
            this.t = new Thread(r, this.coordinatorThreadName);
            this.t.setContextClassLoader(this.cl);
            this.t.setUncaughtExceptionHandler(this.errorHandler);
            return this.t;
        }
    }

    String getCoordinatorThreadName() {
        return this.coordinatorThreadName;
    }

    boolean isCurrentThreadCoordinatorThread() {
        return Thread.currentThread() == this.t;
    }
}
