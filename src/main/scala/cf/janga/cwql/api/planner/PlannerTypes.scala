package cf.janga.cwql.api.planner

sealed trait Result
case class ResultSet(records: Seq[Record]) extends Result
case class InsertResult(success: Boolean) extends Result

case class Record(timestamp: String, data: Map[String, (String, Int)] = Map.empty[String, (String, Int)]) {

  def +(record: Record): Record = {
    Record(timestamp, data ++ record.data)
  }
}

sealed trait ExecutionError
case class CloudWatchClientError(message: String) extends ExecutionError
case object ClientError extends ExecutionError
case object UnknownCwError extends ExecutionError

trait Step {
  def execute(inputOption: Option[Result]): Either[ExecutionError, Result]
}
