/* (C)2021 */
package com.neighborhood.aka.laplace.hackathon.watermark

import org.junit.{Before, Test}
import com.neighborhood.aka.lapalce.hackathon.watermark.TestContext._
import com.neighborhood.aka.laplace.hackathon.watermark.WatermarkAlignSupport.Timestamp
import org.junit.Assert._

class WatermarkAlignSupportTest {

  @Test
  def testRegisterRegisterOperator: Unit = {
    WatermarkAlignSupport.registerOperator(OP1)
    WatermarkAlignSupport.registerOperator(OP2)

    assertEquals(WatermarkAlignSupport.getTsMap.size(), 2)
    assertEquals(WatermarkAlignSupport.getTsMap.get(OP1), Timestamp.EMPTY)
    assertEquals(WatermarkAlignSupport.getTsMap.get(OP2), Timestamp.EMPTY)
  }

}
