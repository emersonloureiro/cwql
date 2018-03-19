package cf.janga.cwql.api.planner

import scala.collection.mutable.{Map => MMap}

class HashJoin {

  private val resultSetMap = MMap.empty[String, Record]

  def +(leadingKey: String, value: Record): Unit = {
    val resultingValue =
      resultSetMap.get(leadingKey) match {
        case None => value
        case Some(existingValue) => existingValue + value
      }
    resultSetMap.put(leadingKey, resultingValue)
  }

  def result(): Seq[Record] = {
    resultSetMap.values.toSeq
  }
}
