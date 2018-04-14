package cf.janga.cwql.integration

import cf.janga.cwql.api.executor.Executor
import cf.janga.cwql.api.parser._
import cf.janga.cwql.api.planner.Planner
import org.scalatest.{Matchers, WordSpec}

import scala.util.Success

class IntegrationTest extends WordSpec with Matchers {

  private def testQuery(query: String) = {
    val Right(_) =
      for {
        parsedQuery <- new Parser().parse(query)
        plan <- new Planner().plan(parsedQuery)
        resultSet <- new Executor().execute(plan.steps)
      } yield {
        println(resultSet)
        resultSet
      }
  }

  "cwql" when {
    "given a AWS namespace" should {
      "return metric statistics" in {
        testQuery {
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
        }
      }
    }

    "given aliases" should {
      "return metric statistics" in {
        testQuery {
          """
            |SELECT
            | max(ec2.CPUUtilization),
            | avg(ec2.CPUUtilization)
            |FROM AWS/EC2 AS ec2
            |WHERE ec2.InstanceId='i-00c753b8c2e2273a9'
            |BETWEEN 2018-03-10T00:00:00Z
            | AND 2018-03-10T14:00:00Z
            |PERIOD 3600
          """.stripMargin
        }
      }
    }
  }
}
