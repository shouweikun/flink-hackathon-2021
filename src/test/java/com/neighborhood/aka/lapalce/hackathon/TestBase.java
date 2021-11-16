package com.neighborhood.aka.lapalce.hackathon;

import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.metrics.jmx.JMXReporter;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.test.util.MiniClusterWithClientResource;

import org.junit.ClassRule;

public abstract class TestBase {

    public static Configuration getFlinkConfiguration() {
        Configuration flinkConfig = new Configuration();
        flinkConfig.set(TaskManagerOptions.MANAGED_MEMORY_SIZE, MemorySize.parse("16m"));
        flinkConfig.setString(
                ConfigConstants.METRICS_REPORTER_PREFIX
                        + "my_reporter."
                        + ConfigConstants.METRICS_REPORTER_CLASS_SUFFIX,
                JMXReporter.class.getName());
        return flinkConfig;
    }

    protected static int NUM_TMS = 1;

    protected static int TM_SLOTS = 1;

    @ClassRule
    public static MiniClusterWithClientResource flink =
            new MiniClusterWithClientResource(
                    new MiniClusterResourceConfiguration.Builder()
                            .setConfiguration(getFlinkConfiguration())
                            .setNumberTaskManagers(NUM_TMS)
                            .setNumberSlotsPerTaskManager(TM_SLOTS)
                            .build());
}
