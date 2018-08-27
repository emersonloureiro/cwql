package cf.janga.cwql.planner

import org.scalatest.{Matchers, WordSpec}

class OrderByStepTest extends WordSpec with Matchers{

  "order by step" should {
    "sort an unsorted result by timestamp" in {
      val orderByStep = new OrderByStep(None)
      val record_1 = Record("2018-03-10T12:55:00Z", Map.empty)
      val record_2 = Record("2018-03-10T12:56:00Z", Map.empty)
      val record_3 = Record("2018-03-10T12:57:00Z", Map.empty)
      val record_4 = Record("2018-03-10T12:59:00Z", Map.empty)
      val resultSet = ResultSet(Seq(record_2, record_4, record_1, record_3))
      val Right(sortedResultSet: ResultSet) = orderByStep.execute(Some(resultSet))
      sortedResultSet.records should be(Seq(record_1, record_2, record_3, record_4))
    }

    "leave a sorted result by timestamp unchanged" in {
      val orderByStep = new OrderByStep(None)
      val record_1 = Record("2018-03-10T12:55:00Z", Map.empty)
      val record_2 = Record("2018-03-10T12:56:00Z", Map.empty)
      val record_3 = Record("2018-03-10T12:57:00Z", Map.empty)
      val record_4 = Record("2018-03-10T12:59:00Z", Map.empty)
      val resultSet = ResultSet(Seq(record_1, record_2, record_3, record_4))
      val Right(sortedResultSet: ResultSet) = orderByStep.execute(Some(resultSet))
      sortedResultSet.records should be(Seq(record_1, record_2, record_3, record_4))
    }
  }
}
