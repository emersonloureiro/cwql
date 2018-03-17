package cf.janga.cwql.integration

import cf.janga.cwql.executor.Executor
import cf.janga.cwql.parser._
import cf.janga.cwql.planner.CwqlPlanner
import org.scalatest.{Matchers, WordSpec}

import scala.util.Success

class IntegrationTest extends WordSpec with Matchers {

  "cwql" when {
    "given a AWS namespace" should {
      "return metric statistics" in {
        val query =
          """
            |SELECT
            | max(CPUUtilization),
            | avg(CPUUtilization)
            |FROM AWS/EC2
            |WHERE InstanceId='i-00c753b8c2e2273a9'
            |BETWEEN 2018-03-10T00:00:00Z
            | AND 2018-03-10T14:00:00Z
            |PERIOD 3600
          """.stripMargin

        val Success(resultSet) =
          for {
            parsedQuery <- new Parser().parse(query)
            plan <- new CwqlPlanner().plan(parsedQuery)
            resultSet <- new Executor().execute(plan.steps)
          } yield {
            resultSet
          }
        println(resultSet)
      }
    }
  }
}
