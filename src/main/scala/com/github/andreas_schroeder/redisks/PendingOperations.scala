package com.github.andreas_schroeder.redisks

import java.util.concurrent.ArrayBlockingQueue

import scala.annotation.tailrec
import scala.concurrent.Future

trait PendingOperations {

  private val pendingOperations = new ArrayBlockingQueue[Future[Any]](50)

  @tailrec
  final def waitOnPendingOperations(wait: Boolean = false): Unit =
    if (!pendingOperations.isEmpty) {
      if (wait) {
        Thread.sleep(10)
      }
      purgeCompletedOperations()
      waitOnPendingOperations(wait = true)
    }

  def addPendingOperations(f: Future[Any]): Unit =
    while (!pendingOperations.offer(f)) {
      purgeCompletedOperations()
    }

  private def purgeCompletedOperations(): Unit =
    pendingOperations.forEach { f =>
      if (f.isCompleted) {
        pendingOperations.remove(f)
      }
    }
}
