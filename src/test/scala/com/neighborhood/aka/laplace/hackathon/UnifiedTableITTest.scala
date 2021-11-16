/* (C)2021 */
package com.neighborhood.aka.laplace.hackathon

import com.neighborhood.aka.lapalce.hackathon.TestBase
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment
import org.junit.{Before, Test}

class UnifiedTableITTest extends TestBase {

  protected var env: StreamExecutionEnvironment = null
  protected var tEnv: StreamTableEnvironment = null

  @Before
  def setup(): Unit = {
    env = StreamExecutionEnvironment.getExecutionEnvironment
    tEnv = StreamTableEnvironment.create(
      env,
      EnvironmentSettings.newInstance.useBlinkPlanner.inStreamingMode.build
    )
    env.getConfig.setRestartStrategy(RestartStrategies.noRestart)
    env.setParallelism(1)
  }

  @Test
  def testUnifiedTableSource(): Unit = {}

}
