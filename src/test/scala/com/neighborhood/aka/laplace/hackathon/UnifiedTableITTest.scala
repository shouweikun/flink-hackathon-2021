/* (C)2021 */
package com.neighborhood.aka.laplace.hackathon

import com.neighborhood.aka.lapalce.hackathon.{TestBase, UnifiedTableFactory}
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment
import org.apache.flink.table.factories.FactoryUtil
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
  def testUnifiedTableSource(): Unit = {

    val sourceTableName = "source_table"
    val createTableSql =
      s"""
         create table $sourceTableName(
          k int,
          v int,
          primary key(k) not enforced
         ) with (
          'connector' = 'unified',
          '${UnifiedTableFactory.CHANGELOG_PREFIX}connector' = 'dummy',
          '${UnifiedTableFactory.BULK_PREFIX}connector' = 'dummy',
          '${UnifiedTableFactory.CHANGELOG_PREFIX}${FactoryUtil.FORMAT
        .key()}' = 'TEST-CHANGELOG',
          '${UnifiedTableFactory.WATERMARK_ALIGN.key()}' = 'false',
          '${UnifiedTableFactory.BULK_PREFIX}${FactoryUtil.FORMAT
        .key()}' = 'TEST-BULK'
         )
        """

    println(createTableSql)

    tEnv.executeSql(createTableSql)

    tEnv.executeSql(s"select count(*) from $sourceTableName").print()
  }

  @Test
  def testUnifiedTableSourceWithWatermarkAlign(): Unit = {

    val sourceTableName = "source_table"
    val createTableSql =
      s"""
         create table $sourceTableName(
          k int,
          v int,
          primary key(k) not enforced
         ) with (
          'connector' = 'unified',
          '${UnifiedTableFactory.CHANGELOG_PREFIX}connector' = 'dummy',
          '${UnifiedTableFactory.BULK_PREFIX}connector' = 'dummy',
          '${UnifiedTableFactory.CHANGELOG_PREFIX}${FactoryUtil.FORMAT
        .key()}' = 'TEST-CHANGELOG',
          '${UnifiedTableFactory.WATERMARK_ALIGN.key()}' = 'true',
          '${UnifiedTableFactory.BULK_PREFIX}${FactoryUtil.FORMAT
        .key()}' = 'TEST-BULK'
         )
        """

    println(createTableSql)

    tEnv.executeSql(createTableSql)

    tEnv.executeSql(s"select distinct(k) from $sourceTableName").print()
  }

}
