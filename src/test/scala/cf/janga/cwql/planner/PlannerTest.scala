package cf.janga.cwql.planner

import cf.janga.cwql.parser._
import org.joda.time.DateTime
import org.joda.time.format.ISODateTimeFormat
import org.scalatest.{Matchers, WordSpec}

import scala.collection.JavaConverters._
import scala.util.{Failure, Success}

class PlannerTest extends WordSpec with Matchers {

  "cqwl planner" when {
    "given a cw query" should {
      "plan a single request for a single statistic of a single metric" in {
        val projection = Projection(Statistic("avg"), None, "time")
        val namespace = Namespace("AWS/EC2")
        val between = Between("2018-01-01T00:00:00Z", "2018-01-31T23:59:59Z")
        val period = Period(60)
        val cwQuery = CwQuery(List(projection), List(namespace), None, between, period)
        val Success(cwQueryPlan) = new CwqlPlanner().plan(cwQuery)
        cwQueryPlan.steps.size should be(1)
        val CwRequestStep(_, cwRequests) = cwQueryPlan.steps.head
        cwRequests.size should be(1)
        val cwRequest = cwRequests.head
        cwRequest.getNamespace should be(namespace.value)
        cwRequest.getStatistics.asScala should be(List("Average"))
        cwRequest.getMetricName should be(projection.metric)
        cwRequest.getPeriod should be(period.value)
        val formatter = ISODateTimeFormat.dateTimeNoMillis()
        val startDateTime = formatter.parseDateTime(cwQuery.between.startTime)
        new DateTime(cwRequest.getStartTime) should be(startDateTime)
        val endDateTime = formatter.parseDateTime(cwQuery.between.endTime)
        new DateTime(cwRequest.getEndTime) should be(endDateTime)
      }

      "plan a single request for multiple statistics of a single metric" in {
        val avgProjection = Projection(Statistic("avg"), None, "time")
        val sumProjection = Projection(Statistic("sum"), None, "time")
        val namespace = Namespace("AWS/EC2")
        val between = Between("2018-01-01T00:00:00Z", "2018-01-31T23:59:59Z")
        val period = Period(60)
        val cwQuery = CwQuery(List(avgProjection, sumProjection), List(namespace), None, between, period)
        val Success(cwQueryPlan) = new CwqlPlanner().plan(cwQuery)
        cwQueryPlan.steps.size should be(1)
        val CwRequestStep(_, cwRequests) = cwQueryPlan.steps.head
        cwRequests.size should be(1)
        val cwRequest = cwRequests.head
        cwRequest.getNamespace should be(namespace.value)
        cwRequest.getStatistics.asScala.contains("Average") should be(true)
        cwRequest.getStatistics.asScala.contains("Sum") should be(true)
        cwRequest.getMetricName should be(avgProjection.metric)
        cwRequest.getPeriod should be(period.value)
        val formatter = ISODateTimeFormat.dateTimeNoMillis()
        val startDateTime = formatter.parseDateTime(cwQuery.between.startTime)
        new DateTime(cwRequest.getStartTime) should be(startDateTime)
        val endDateTime = formatter.parseDateTime(cwQuery.between.endTime)
        new DateTime(cwRequest.getEndTime) should be(endDateTime)
      }

      "plan multiple requests for multiple metrics" in {
        val avgProjection = Projection(Statistic("avg"), None, "time")
        val sumProjection = Projection(Statistic("sum"), None, "request_size")
        val namespace = Namespace("AWS/EC2")
        val between = Between("2018-01-01T00:00:00Z", "2018-01-31T23:59:59Z")
        val period = Period(60)
        val cwQuery = CwQuery(List(avgProjection, sumProjection), List(namespace), None, between, period)
        val Success(cwQueryPlan) = new CwqlPlanner().plan(cwQuery)
        cwQueryPlan.steps.size should be(1)
        val List(CwRequestStep(_, cwRequests)) = cwQueryPlan.steps
        cwRequests.size should be(2)
        val avgCwRequest = cwRequests(0)
        avgCwRequest.getNamespace should be(namespace.value)
        avgCwRequest.getStatistics.asScala should be(List("Average"))
        avgCwRequest.getMetricName should be(avgProjection.metric)
        avgCwRequest.getPeriod should be(period.value)
        val formatter = ISODateTimeFormat.dateTimeNoMillis()
        val avgRequestStartDateTime = formatter.parseDateTime(cwQuery.between.startTime)
        new DateTime(avgCwRequest.getStartTime) should be(avgRequestStartDateTime)
        val avgRequestEndDateTime = formatter.parseDateTime(cwQuery.between.endTime)
        new DateTime(avgCwRequest.getEndTime) should be(avgRequestEndDateTime)

        val sumCwRequest = cwRequests(1)
        sumCwRequest.getNamespace should be(namespace.value)
        sumCwRequest.getStatistics.asScala should be(List("Sum"))
        sumCwRequest.getMetricName should be(sumProjection.metric)
        sumCwRequest.getPeriod should be(period.value)
        val sumRequesStartDateTime = formatter.parseDateTime(cwQuery.between.startTime)
        new DateTime(sumCwRequest.getStartTime) should be(sumRequesStartDateTime)
        val sumRequestEndDateTime = formatter.parseDateTime(cwQuery.between.endTime)
        new DateTime(sumCwRequest.getEndTime) should be(sumRequestEndDateTime)
      }

      "fail when start time is after end time" in {
        val avgProjection = Projection(Statistic("avg"), None, "time")
        val sumProjection = Projection(Statistic("sum"), None, "request_size")
        val namespace = Namespace("AWS/EC2")
        val between = Between("2018-01-01T10:00:00Z", "2018-01-01T00:00:00Z")
        val period = Period(60)
        val cwQuery = CwQuery(List(avgProjection, sumProjection), List(namespace), None, between, period)
        val Failure(_) = new CwqlPlanner().plan(cwQuery)
      }
    }
  }
}
