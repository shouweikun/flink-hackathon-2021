/* (C)2021 */
package com.neighborhood.aka.laplace.hackathon.watermark

import org.junit.{AfterClass, BeforeClass, Test}
import com.neighborhood.aka.lapalce.hackathon.watermark.TestContext._
import com.neighborhood.aka.laplace.hackathon.watermark.WatermarkAlignSupport.{
  Timestamp,
  getGlobalTs,
  getGlobalTsInternal,
  getTsMap
}
import org.junit.Assert._

import java.lang.{Long => JLong}
import WatermarkAlignSupportTest._

object WatermarkAlignSupportTest {
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
    WatermarkAlignSupport.putOperatorTs(OP1, setupTs + 10)
    WatermarkAlignSupport.putOperatorTs(OP2, setupTs + 9)
    assertEquals(getGlobalTsInternal(), setupTs + 9)
    assertEquals(getGlobalTs(), setupTs + 8)

    WatermarkAlignSupport.putOperatorTs(OP2, setupTs + 11)
    assertEquals(getGlobalTsInternal(), setupTs + 10)
    assertEquals(getGlobalTs(), setupTs + 9)
  }

}
