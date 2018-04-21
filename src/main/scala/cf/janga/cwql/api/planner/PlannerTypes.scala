package cf.janga.cwql.api.planner

case class ResultSet(records: Seq[Record])

case class Record(timestamp: String, data: Map[String, (String, Int)] = Map.empty[String, (String, Int)]) {

  def +(record: Record): Record = {
    Record(timestamp, data ++ record.data)
  }
}

sealed trait ExecutionError
case class CloudWatchClientError(message: String) extends ExecutionError

trait Step {
  def execute(inputOption: Option[ResultSet]): Either[ExecutionError, ResultSet]
}
