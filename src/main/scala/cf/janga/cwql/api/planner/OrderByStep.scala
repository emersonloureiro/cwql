package cf.janga.cwql.api.planner

case class OrderByStep(leadingKeyOption: Option[String]) extends Step {

  def execute(resultSetOption: Option[ResultSet]): ResultSet = resultSetOption match {
    case None => sys.error("Order by expects a result set")
    case Some(resultSet) => {
      leadingKeyOption match {
        case Some(leadingKey) => sys.error("Order by for specific column not supported")
        case None => {
          val sortedRecords = resultSet.records.sortWith((r1, r2) => r1.timestamp < r2.timestamp)
          ResultSet(sortedRecords)
        }
      }
    }
  }
}
