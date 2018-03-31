package cf.janga.cwql.api.planner

case class ResultSet(records: Seq[Record])

case class Record(timestamp: String, data: Map[String, String] = Map.empty[String, String]) {

  def +(record: Record): Record = {
    Record(timestamp, data ++ record.data)
  }
}

trait Step {
  def execute(inputOption: Option[ResultSet]): ResultSet
}
