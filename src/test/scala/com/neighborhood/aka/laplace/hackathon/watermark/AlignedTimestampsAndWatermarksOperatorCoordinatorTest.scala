/* (C)2021 */
package com.neighborhood.aka.laplace.hackathon.watermark

import com.neighborhood.aka.lapalce.hackathon.watermark.CoordinatorExecutorThreadFactory
import com.neighborhood.aka.lapalce.hackathon.watermark.TestContext.OP1
import org.apache.flink.runtime.operators.coordination.{
  EventReceivingTasks,
  MockOperatorCoordinatorContext
}
import org.junit.Assert.{assertNotNull, assertTrue}
import org.junit.{After, Before, Test}

import java.util.concurrent.{
  ExecutorService,
  Executors,
  TimeUnit,
  TimeoutException
}

object AlignedTimestampsAndWatermarksOperatorCoordinatorTest {}

class AlignedTimestampsAndWatermarksOperatorCoordinatorTest {

  val NUM_SUBTASKS = 3
  val operatorName = OP1

  // ---- Mocks for the underlying Operator Coordinator Context ---
  protected var receivingTasks: EventReceivingTasks = null
  protected var operatorCoordinatorContext: MockOperatorCoordinatorContext =
    null

  // ---- Mocks for the Source Coordinator Context ----
  protected var coordinatorThreadFactory: CoordinatorExecutorThreadFactory =
    null
  protected var coordinatorExecutor: ExecutorService = null
  protected var coordinator: AlignedTimestampsAndWatermarksOperatorCoordinator =
    _
  @Before
  @throws[Exception]
  def setup(): Unit = {
    receivingTasks = EventReceivingTasks.createForRunningTasks
    operatorCoordinatorContext =
      new MockOperatorCoordinatorContext(operatorName, NUM_SUBTASKS)
    val coordinatorThreadName = OP1.toHexString
    coordinatorThreadFactory = new CoordinatorExecutorThreadFactory(
      coordinatorThreadName,
      getClass.getClassLoader
    )
    coordinatorExecutor =
      Executors.newSingleThreadExecutor(coordinatorThreadFactory)
    coordinator = getNewCoordinator
  }

  @After
  @throws[InterruptedException]
  @throws[TimeoutException]
  def cleanUp(): Unit = {
    coordinatorExecutor.shutdown()
    WatermarkAlignSupportTest.clear()
    if (!coordinatorExecutor.awaitTermination(10, TimeUnit.SECONDS))
      throw new TimeoutException(
        "Failed to close the CoordinatorExecutor before timeout."
      )
  }

  def getNewCoordinator = {
    new AlignedTimestampsAndWatermarksOperatorCoordinator(
      NUM_SUBTASKS,
      operatorName
    )
  }

  @Test
  def testStart(): Unit = {
    coordinator.start()
    val context = coordinator.getRuntimeContext()
    assertNotNull(context)
    assertTrue(WatermarkAlignSupport.getTsMap.containsKey(OP1));
  }

}
