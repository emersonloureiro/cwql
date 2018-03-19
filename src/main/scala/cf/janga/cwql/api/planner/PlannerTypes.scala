package cf.janga.cwql.api.planner

import scala.util.Try

case class ResultSet(records: Seq[Record])

case class Record(values: Map[String, String] = Map.empty[String, String]) {

  def +(record: Record): Record = {
    Record(values ++ record.values)
  }
}

trait Step {
  def execute(inputOption: Option[ResultSet]): Try[ResultSet]
}
