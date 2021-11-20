/* (C)2021 */
package com.neighborhood.aka.lapalce.hackathon.watermark

import com.neighborhood.aka.laplace.hackathon.watermark.LocalWatermarkRequestAck
import org.apache.flink.runtime.jobgraph.OperatorID
import org.apache.flink.runtime.operators.coordination.{
  OperatorCoordinator,
  OperatorEvent
}

import java.util.concurrent.CompletableFuture
import java.lang.{Long => JLong}

class AlignedTimestampsAndWatermarksOperatorCoordinator(
    private val parallelism: Int,
    private val operatorID: OperatorID
) extends OperatorCoordinator {

  private val subtaskGateways = Array.fill[OperatorCoordinator.SubtaskGateway](
    parallelism
  )(null: OperatorCoordinator.SubtaskGateway)

  private var currOperatorWatermarkTs: JLong = _

  override def start(): Unit = ???

  override def close(): Unit = ???

  override def handleEventFromOperator(
      subtaskId: Int,
      operatorEvent: OperatorEvent
  ): Unit = {
    operatorEvent match {
      case event: LocalWatermarkRequestAck =>
      case _                               =>
    }

  }

  override def checkpointCoordinator(
      l: Long,
      completableFuture: CompletableFuture[Array[Byte]]
  ): Unit = ???

  override def subtaskReady(
      subTaskId: Int,
      subtaskGateway: OperatorCoordinator.SubtaskGateway
  ): Unit = {
    subtaskGateways(subTaskId) = subtaskGateway
  }

  override def notifyCheckpointComplete(l: Long): Unit = ???

  override def resetToCheckpoint(l: Long, bytes: Array[Byte]): Unit = ???

  override def subtaskFailed(i: Int, throwable: Throwable): Unit = ???

  override def subtaskReset(i: Int, l: Long): Unit = ???
}
