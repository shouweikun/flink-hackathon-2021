/* (C)2021 */
package com.neighborhood.aka.lapalce.hackathon.watermark

import com.neighborhood.aka.laplace.hackathon.watermark.AlignedTimestampsAndWatermarksOperatorCoordinator
import org.apache.flink.runtime.jobgraph.OperatorID
import org.apache.flink.runtime.operators.coordination.OperatorCoordinator

import java.util.function.Consumer

class AlignedTimestampsAndWatermarksOperatorCoordinatorProvider(
    final val operatorId: OperatorID
) extends OperatorCoordinator.Provider {

  override def getOperatorId: OperatorID = operatorId

  override def create(
      context: OperatorCoordinator.Context
  ): OperatorCoordinator = {
    val parallelism = context.currentParallelism()
    new AlignedTimestampsAndWatermarksOperatorCoordinator(
      parallelism,
      getOperatorId,
      new Consumer[Throwable] {
        override def accept(t: Throwable): Unit = context.failJob(t)
      }
    )
  }
}
