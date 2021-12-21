/* (C)2021 */
package com.neighborhood.aka.laplace.hackathon.watermark

import com.neighborhood.aka.lapalce.hackathon.watermark.CoordinatorExecutorThreadFactory
import com.neighborhood.aka.lapalce.hackathon.watermark.TestContext.OP1
import org.apache.flink.runtime.operators.coordination.{
  EventReceivingTasks,
  MockOperatorCoordinatorContext
}
import org.junit.Assert.{assertEquals, assertNotNull, assertNull, assertTrue}
import org.junit.{After, Before, Test}

import java.util.concurrent.{
  CompletableFuture,
  CountDownLatch,
  ExecutorService,
  Executors,
  TimeUnit,
  TimeoutException
}
import java.util.function.Consumer

object AlignedTimestampsAndWatermarksOperatorCoordinatorTest {}

class AlignedTimestampsAndWatermarksOperatorCoordinatorTest {

  val NUM_SUBTASKS = 3
  val operatorName = OP1

  // ---- Mocks for the underlying Coordinator Context ---
  protected var receivingTasks: EventReceivingTasks = null
  protected var operatorCoordinatorContext: MockOperatorCoordinatorContext =
    null

  // ---- Mocks for the Coordinator Context ----
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
      operatorName,
      new Consumer[Throwable] {
        override def accept(t: Throwable): Unit = {}
      }
    )
  }

  @throws[Exception]
  def subtaskReady(): Unit = {
    coordinator.start()
    setAllReaderTasksReady(coordinator)
  }

  def setAllReaderTasksReady(): Unit = {
    setAllReaderTasksReady(coordinator)
  }

  def setAllReaderTasksReady(
      coordinator: AlignedTimestampsAndWatermarksOperatorCoordinator
  ): Unit = {
    for (i <- 0 until NUM_SUBTASKS) {
      coordinator.subtaskReady(i, receivingTasks.createGatewayForSubtask(i))
    }
  }

  @Test
  def testStart(): Unit = {
    coordinator.start()
    val context = coordinator.getRuntimeContext()
    assertNotNull(context)
    assertTrue(WatermarkAlignSupport.getTsMap.containsKey(OP1));
  }

  @Test
  def testSubtaskReady: Unit = {
    subtaskReady()
    val gateways = coordinator.getSubtaskGateways
    assertEquals(NUM_SUBTASKS, gateways.length)
    gateways.foreach(assertNotNull)
  }

  @Test
  def testSendOperatorEvent(): Unit = {
    subtaskReady()
    val ts = System.currentTimeMillis()
    (0 until NUM_SUBTASKS).foreach {
      case index =>
        coordinator.handleEventFromOperator(
          index,
          new ReportLocalWatermark(index, ts)
        )
        assertEquals(
          coordinator.getRuntimeContext.subtaskIdAndLocalWatermark.get(index),
          ts
        )
    }
    assertEquals(coordinator.computeOperatorTs(), ts)
    assertEquals(coordinator.getGlobalWatermark, ts - 1)

    coordinator.alignCountDownLatch = new CountDownLatch(NUM_SUBTASKS)
    (0 until NUM_SUBTASKS) foreach {
      case index =>
        coordinator.handleEventFromOperator(
          index,
          new WatermarkAlignAck(index, ts)
        )
        assertEquals(
          NUM_SUBTASKS - index - 1,
          coordinator.alignCountDownLatch.getCount
        )
    }

  }

  @Test
  def testCheckpointCoordinator(): Unit = {
    subtaskReady()
    val ts = System.currentTimeMillis()
    val result = new CompletableFuture[Array[Byte]]
    coordinator.checkpointCoordinator(1, result)
    (0 until NUM_SUBTASKS) foreach {
      case index =>
        coordinator.handleEventFromOperator(
          index,
          new WatermarkAlignAck(index, ts)
        )
    }
    assertNotNull(result.get())
    assertNull(coordinator.getGlobalWatermark)

    (0 until NUM_SUBTASKS).foreach {
      case index =>
        coordinator.handleEventFromOperator(
          index,
          new ReportLocalWatermark(index, ts)
        )
    }
    assertNull(coordinator.getGlobalWatermark)
  }

  @Test
  def testNotifyCheckpointComplete(): Unit = {
    subtaskReady()
    val checkpointId = 1
    val ts = System.currentTimeMillis()
    val result = new CompletableFuture[Array[Byte]]()
    assertNull(coordinator.getGlobalWatermark)
    coordinator.checkpointCoordinator(checkpointId, result)
    assertNull(coordinator.getGlobalWatermark)
    (0 until NUM_SUBTASKS).foreach {
      case index =>
        coordinator.handleEventFromOperator(
          index,
          new ReportLocalWatermark(index, ts)
        )
        coordinator.handleEventFromOperator(
          index,
          new WatermarkAlignAck(index, ts)
        )
    }
    assertNull(coordinator.getGlobalWatermark)
    result.get
    coordinator.notifyCheckpointComplete(checkpointId)
    assertEquals(ts, coordinator.getGlobalWatermark + 1)
  }
}
