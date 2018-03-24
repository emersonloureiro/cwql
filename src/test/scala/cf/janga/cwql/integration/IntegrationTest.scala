package cf.janga.cwql.integration

import cf.janga.cwql.api.executor.Executor
import cf.janga.cwql.api.parser._
import cf.janga.cwql.api.planner.Planner
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
            |BETWEEN 2018-03-10T13:00:00Z
            | AND 2018-03-10T14:00:00Z
            |PERIOD 60
          """.stripMargin

        val Right(_) =
          for {
            parsedQuery <- new Parser().parse(query)
            plan <- new Planner().plan(parsedQuery)
            resultSet <- new Executor().execute(plan.steps).toEither
          } yield {
            println(resultSet)
            resultSet
          }
      }
    }
  }
}
