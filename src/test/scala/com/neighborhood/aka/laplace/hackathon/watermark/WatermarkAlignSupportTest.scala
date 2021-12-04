/* (C)2021 */
package com.neighborhood.aka.laplace.hackathon.watermark

import org.junit.{AfterClass, BeforeClass, Test}
import com.neighborhood.aka.lapalce.hackathon.watermark.TestContext._
import com.neighborhood.aka.laplace.hackathon.watermark.WatermarkAlignSupport.{
  Timestamp,
  checkpointCoordinator,
  getGlobalTs,
  getGlobalTsInternal,
  getTsMap,
  notifyCheckpointComplete
}
import org.junit.Assert._

import java.lang.{Long => JLong}
import WatermarkAlignSupportTest._

import java.util.concurrent.atomic.AtomicLong

object WatermarkAlignSupportTest {

  private val checkpointId = new AtomicLong(0)

  private var setupTs: JLong = _

  @BeforeClass
  def setup(): Unit = {
    setupTs = System.currentTimeMillis()
  }

  @AfterClass
  def clear(): Unit = {
    WatermarkAlignSupport.getTsMap.clear()
    WatermarkAlignSupport.currentCheckpointIdAndAlignTs = null
    setupTs = null

  }
}
class WatermarkAlignSupportTest {
  @Test
  def testRegisterRegisterOperator: Unit = {
    WatermarkAlignSupport.registerOperator(OP1)
    WatermarkAlignSupport.registerOperator(OP2)

    assertEquals(WatermarkAlignSupport.getTsMap.size(), 2)
    assertEquals(WatermarkAlignSupport.getTsMap.get(OP1), Timestamp.EMPTY)
    assertEquals(WatermarkAlignSupport.getTsMap.get(OP2), Timestamp.EMPTY)
  }

  @Test
  def testPutOperatorTs(): Unit = {
    WatermarkAlignSupport.putOperatorTs(OP1, setupTs)
    assertEquals(WatermarkAlignSupport.getTsMap.get(OP1).getTs, setupTs)
    WatermarkAlignSupport.putOperatorTs(OP2, null)
    assertEquals(WatermarkAlignSupport.getTsMap.get(OP2).getTs, null)
    WatermarkAlignSupport.putOperatorTs(OP2, setupTs)
    assertEquals(WatermarkAlignSupport.getTsMap.get(OP2).getTs, setupTs)
  }

  @Test
  def testGetGlobalTs(): Unit = {

    WatermarkAlignSupport.putOperatorTs(OP1, null)
    assertEquals(getGlobalTsInternal(), null)

    WatermarkAlignSupport.putOperatorTs(OP1, Long.MinValue)
    WatermarkAlignSupport.putOperatorTs(OP2, Long.MinValue)
    assertEquals(getGlobalTsInternal(), Long.MinValue)

    WatermarkAlignSupport.putOperatorTs(OP1, setupTs + 10)
    WatermarkAlignSupport.putOperatorTs(OP2, setupTs + 9)
    assertEquals(getGlobalTsInternal(), setupTs + 9)
    assertEquals(getGlobalTs(), setupTs + 8)

    WatermarkAlignSupport.putOperatorTs(OP2, setupTs + 11)
    assertEquals(getGlobalTsInternal(), setupTs + 10)
    assertEquals(getGlobalTs(), setupTs + 9)
  }

  @Test
  def testSubtaskCheckpointCoordinator(): Unit = {

    checkpointCoordinator(checkpointId.incrementAndGet())
    assertNotNull(WatermarkAlignSupport.currentCheckpointIdAndAlignTs)
    assertEquals(
      WatermarkAlignSupport.currentCheckpointIdAndAlignTs.alignTs,
      getGlobalTs
    )
    assertEquals(
      WatermarkAlignSupport.currentCheckpointIdAndAlignTs.checkpointId,
      checkpointId.get()
    )
    val before = getGlobalTs
    WatermarkAlignSupport.putOperatorTs(OP1, setupTs + 100)
    val after = getGlobalTs

    assertEquals(before, after)
    assertFalse(checkpointCoordinator(checkpointId.get()))
  }

  @Test
  def testNotifyCheckpointComplete(): Unit = {
    testSubtaskCheckpointCoordinator()
    assertTrue(notifyCheckpointComplete(checkpointId.get()))

    val before = getGlobalTs
    WatermarkAlignSupport.putOperatorTs(OP1, setupTs + 1000)
    WatermarkAlignSupport.putOperatorTs(OP2, setupTs + 1000)
    val after = getGlobalTs

    assertNotEquals(before, after)
    assertFalse(notifyCheckpointComplete(checkpointId.get()))
  }

}
