package cf.janga.cwql.planner

case class OrderByStep(leadingKeyOption: Option[String]) extends Step {

  def execute(resultSetOption: Option[Result]): Either[ExecutionError, Result] = resultSetOption match {
    case None => sys.error("Order by expects a result set")
    case Some(resultSet: ResultSet) => {
      leadingKeyOption match {
        case Some(leadingKey) => sys.error("Order by for specific column not supported")
        case None => {
          val sortedRecords = resultSet.records.sortWith((r1, r2) => r1.timestamp < r2.timestamp)
          Right(ResultSet(sortedRecords))
        }
      }
    }
  }
}
