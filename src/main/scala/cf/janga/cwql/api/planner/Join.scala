package cf.janga.cwql.api.planner

import scala.collection.mutable.{Map => MMap}

class HashJoin {

  private val recordsMap = MMap.empty[String, Record]

  def +(leadingKey: String, record: Record): Unit = {
    val resultingRecord =
      recordsMap.get(leadingKey) match {
        case None => record
        case Some(existingRecord) => existingRecord + record
      }
    recordsMap.put(leadingKey, resultingRecord)
  }

  def result(): Seq[Record] = {
    recordsMap.values.toSeq
  }
}
