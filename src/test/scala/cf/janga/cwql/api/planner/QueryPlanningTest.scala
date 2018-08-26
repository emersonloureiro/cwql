package cf.janga.cwql.api.planner

import cf.janga.cwql.api.parser._
import org.joda.time.DateTime
import org.joda.time.format.ISODateTimeFormat
import org.scalatest.{Matchers, WordSpec}

import scala.collection.JavaConverters._

class QueryPlanningTest extends WordSpec with Matchers {

  "cqwl planner" when {
    "given a single namespace" should {
      "plan a single request for a single statistic of a single metric" in {
        val projection = Projection(Statistic("avg"), None, None, "time")
        val namespace = Namespace("AWS/EC2", None)
        val between = Between("2018-01-01T00:00:00Z", "2018-01-31T23:59:59Z")
        val period = Period(60)
        val cwQuery = Query(List(projection), List(namespace), None, between, period)
        val Right(cwqlPlan) = new Planner().plan(cwQuery)
        cwqlPlan.steps.size should be(2)
        val GetMetricStatisticsStep(_, cwRequests) = cwqlPlan.steps.head
        cwRequests.size should be(1)
        val cwRequest = cwRequests.head.cloudWatchRequest
        cwRequest.getNamespace should be(namespace.value)
        cwRequest.getStatistics.asScala should be(List("Average"))
        cwRequest.getMetricName should be(projection.metric)
        cwRequest.getPeriod should be(period.value)
        val formatter = ISODateTimeFormat.dateTimeNoMillis()
        val startDateTime = formatter.parseDateTime(cwQuery.between.startTime)
        new DateTime(cwRequest.getStartTime) should be(startDateTime)
        val endDateTime = formatter.parseDateTime(cwQuery.between.endTime)
        new DateTime(cwRequest.getEndTime) should be(endDateTime)

        val OrderByStep(None) = cwqlPlan.steps(1)
      }

      "plan a single request for multiple statistics of a single metric" in {
        val avgProjection = Projection(Statistic("avg"), None, None, "time")
        val sumProjection = Projection(Statistic("sum"), None, None, "time")
        val namespace = Namespace("AWS/EC2", None)
        val between = Between("2018-01-01T00:00:00Z", "2018-01-31T23:59:59Z")
        val period = Period(60)
        val cwQuery = Query(List(avgProjection, sumProjection), List(namespace), None, between, period)
        val Right(cwqlPlan) = new Planner().plan(cwQuery)
        cwqlPlan.steps.size should be(2)
        val GetMetricStatisticsStep(_, cwRequests) = cwqlPlan.steps.head
        cwRequests.size should be(1)
        val cwRequest = cwRequests.head.cloudWatchRequest
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

        val OrderByStep(None) = cwqlPlan.steps(1)
      }

      "plan multiple requests for multiple metrics" in {
        val avgProjection = Projection(Statistic("avg"), None, None, "time")
        val sumProjection = Projection(Statistic("sum"), None, None, "request_size")
        val namespace = Namespace("AWS/EC2", None)
        val between = Between("2018-01-01T00:00:00Z", "2018-01-31T23:59:59Z")
        val period = Period(60)
        val cwQuery = Query(List(avgProjection, sumProjection), List(namespace), None, between, period)
        val Right(cwqlPlan) = new Planner().plan(cwQuery)
        cwqlPlan.steps.size should be(2)
        val GetMetricStatisticsStep(_, cwRequests) = cwqlPlan.steps.head
        cwRequests.size should be(2)
        val avgCwRequest = cwRequests.head.cloudWatchRequest
        avgCwRequest.getNamespace should be(namespace.value)
        avgCwRequest.getStatistics.asScala should be(List("Average"))
        avgCwRequest.getMetricName should be(avgProjection.metric)
        avgCwRequest.getPeriod should be(period.value)
        val formatter = ISODateTimeFormat.dateTimeNoMillis()
        val avgRequestStartDateTime = formatter.parseDateTime(cwQuery.between.startTime)
        new DateTime(avgCwRequest.getStartTime) should be(avgRequestStartDateTime)
        val avgRequestEndDateTime = formatter.parseDateTime(cwQuery.between.endTime)
        new DateTime(avgCwRequest.getEndTime) should be(avgRequestEndDateTime)

        val sumCwRequest = cwRequests(1).cloudWatchRequest
        sumCwRequest.getNamespace should be(namespace.value)
        sumCwRequest.getStatistics.asScala should be(List("Sum"))
        sumCwRequest.getMetricName should be(sumProjection.metric)
        sumCwRequest.getPeriod should be(period.value)
        val sumRequesStartDateTime = formatter.parseDateTime(cwQuery.between.startTime)
        new DateTime(sumCwRequest.getStartTime) should be(sumRequesStartDateTime)
        val sumRequestEndDateTime = formatter.parseDateTime(cwQuery.between.endTime)
        new DateTime(sumCwRequest.getEndTime) should be(sumRequestEndDateTime)

        val OrderByStep(None) = cwqlPlan.steps(1)
      }

      "fail when namespace alias isn't used on projections" in {
        val avgProjection = Projection(Statistic("avg"), None, None, "time")
        val sumProjection = Projection(Statistic("sum"), None, None, "time")
        val namespace = Namespace("AWS/EC2", Some("ec2"))
        val between = Between("2018-01-01T00:00:00Z", "2018-01-31T23:59:59Z")
        val period = Period(60)
        val cwQuery = Query(List(avgProjection, sumProjection), List(namespace), None, between, period)
        val Left(planningError) = new Planner().plan(cwQuery)
        planningError should be(UnmatchedProjection(avgProjection))
      }

      "fail when namespace alias isn't used on selection" in {
        val avgProjection = Projection(Statistic("avg"), Some("ec2"), None, "time")
        val sumProjection = Projection(Statistic("sum"), Some("ec2"), None, "time")
        val namespace = Namespace("AWS/EC2", Some("ec2"))
        val between = Between("2018-01-01T00:00:00Z", "2018-01-31T23:59:59Z")
        val period = Period(60)
        val booleanExpression = SimpleBooleanExpression(None, "InstanceId", Equals, StringValue("123456"))
        val selection = Selection(BooleanExpression(booleanExpression, Seq.empty))
        val cwQuery = Query(List(avgProjection, sumProjection), List(namespace), Some(selection), between, period)
        val Left(planningError) = new Planner().plan(cwQuery)
        planningError should be(UnmatchedFilter(booleanExpression))
      }

      "fail when namespace alias isn't provided but it's used on projections" in {
        val avgProjection = Projection(Statistic("avg"), Some("ec2"), None, "time")
        val sumProjection = Projection(Statistic("sum"), Some("ec2"), None, "time")
        val namespace = Namespace("AWS/EC2", None)
        val between = Between("2018-01-01T00:00:00Z", "2018-01-31T23:59:59Z")
        val period = Period(60)
        val cwQuery = Query(List(avgProjection, sumProjection), List(namespace), None, between, period)
        val Left(planningError) = new Planner().plan(cwQuery)
        planningError should be(UnmatchedProjection(avgProjection))
      }

      "plan a request with namespace aliases" in {
        val avgProjection = Projection(Statistic("avg"), Some("ec2"), None, "time")
        val sumProjection = Projection(Statistic("sum"), Some("ec2"), None, "time")
        val namespace = Namespace("AWS/EC2", Some("ec2"))
        val between = Between("2018-01-01T00:00:00Z", "2018-01-31T23:59:59Z")
        val period = Period(60)
        val cwQuery = Query(List(avgProjection, sumProjection), List(namespace), None, between, period)
        val Right(cwqlPlan) = new Planner().plan(cwQuery)
        cwqlPlan.steps.size should be(2)
        val GetMetricStatisticsStep(_, cwRequests) = cwqlPlan.steps.head
        cwRequests.size should be(1)
        val cwRequest = cwRequests.head.cloudWatchRequest
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

        val OrderByStep(None) = cwqlPlan.steps(1)
      }

      "fail when the same alias is used in different projections" in {
        val ec2AvgTimeProjection = Projection(Statistic("avg"), None, Some("avg_time"), "time")
        val ec2AvgLatencyProjection = Projection(Statistic("avg"), None, Some("avg_time"), "latency")
        val ec2Namespace = Namespace("AWS/EC2", None)
        val between = Between("2018-01-01T00:00:00Z", "2018-01-31T23:59:59Z")
        val period = Period(60)
        val cwQuery = Query(List(ec2AvgTimeProjection, ec2AvgLatencyProjection), List(ec2Namespace), None, between, period)
        val Left(planningError) = new Planner().plan(cwQuery)
        planningError should be(ProjectionAliasAlreadyInUse(ec2AvgLatencyProjection))
      }
    }

    "when given multiple namespaces" should {
      "plan multiple requests for single metric per namespace" in {
        val ec2AvgProjection = Projection(Statistic("avg"), Some("ec2"), None, "time")
        val ec2Namespace = Namespace("AWS/EC2", Some("ec2"))
        val elbAvgProjection = Projection(Statistic("avg"), Some("elb"), None, "latency")
        val elbNamespace = Namespace("AWS/ELB", Some("elb"))
        val between = Between("2018-01-01T00:00:00Z", "2018-01-31T23:59:59Z")
        val period = Period(60)
        val selection = Selection(
          BooleanExpression(SimpleBooleanExpression(Some("ec2"), "InstanceId", Equals, StringValue("123456")),
            Seq((And, SimpleBooleanExpression(Some("elb"), "ElbArn", Equals, StringValue("987654"))))))
        val cwQuery = Query(
          List(ec2AvgProjection, elbAvgProjection), List(ec2Namespace, elbNamespace),
          Some(selection),
          between, period)
        val Right(cwqlPlan) = new Planner().plan(cwQuery)
        cwqlPlan.steps.size should be(2)
        val GetMetricStatisticsStep(_, cwRequests) = cwqlPlan.steps.head
        cwRequests.size should be(2)

        // avg ec2 metric
        val ec2AvgCwRequest = cwRequests.head.cloudWatchRequest
        ec2AvgCwRequest.getNamespace should be(ec2Namespace.value)
        ec2AvgCwRequest.getStatistics.asScala should be(List("Average"))
        ec2AvgCwRequest.getMetricName should be(ec2AvgProjection.metric)
        ec2AvgCwRequest.getPeriod should be(period.value)
        val ec2AvgCwRequestDimensions = ec2AvgCwRequest.getDimensions.asScala
        ec2AvgCwRequestDimensions.size should be(1)
        ec2AvgCwRequestDimensions.head.getName should be("InstanceId")
        ec2AvgCwRequestDimensions.head.getValue should be("123456")
        val formatter = ISODateTimeFormat.dateTimeNoMillis()
        val ec2AvgRequestStartDateTime = formatter.parseDateTime(cwQuery.between.startTime)
        new DateTime(ec2AvgCwRequest.getStartTime) should be(ec2AvgRequestStartDateTime)
        val ec2AvgRequestEndDateTime = formatter.parseDateTime(cwQuery.between.endTime)
        new DateTime(ec2AvgCwRequest.getEndTime) should be(ec2AvgRequestEndDateTime)

        // avg elb metric
        val elbAvgCwRequest = cwRequests(1).cloudWatchRequest
        elbAvgCwRequest.getNamespace should be(elbNamespace.value)
        elbAvgCwRequest.getStatistics.asScala should be(List("Average"))
        elbAvgCwRequest.getMetricName should be(elbAvgProjection.metric)
        elbAvgCwRequest.getPeriod should be(period.value)
        val elbAvgCwRequestDimensions = elbAvgCwRequest.getDimensions.asScala
        elbAvgCwRequestDimensions.size should be(1)
        elbAvgCwRequestDimensions.head.getName should be("ElbArn")
        elbAvgCwRequestDimensions.head.getValue should be("987654")
        val elbAvgRequestStartDateTime = formatter.parseDateTime(cwQuery.between.startTime)
        new DateTime(elbAvgCwRequest.getStartTime) should be(elbAvgRequestStartDateTime)
        val elbAvgRequestEndDateTime = formatter.parseDateTime(cwQuery.between.endTime)
        new DateTime(elbAvgCwRequest.getEndTime) should be(elbAvgRequestEndDateTime)

        val OrderByStep(None) = cwqlPlan.steps(1)
      }

      "fail for multiple namespaces without aliases" in {
        val ec2AvgProjection = Projection(Statistic("avg"), None, None, "time")
        val ec2Namespace = Namespace("AWS/EC2", None)
        val elbAvgProjection = Projection(Statistic("avg"), None, None, "latency")
        val elbNamespace = Namespace("AWS/ELB", None)
        val between = Between("2018-01-01T00:00:00Z", "2018-01-31T23:59:59Z")
        val period = Period(60)
        val cwQuery = Query(List(ec2AvgProjection, elbAvgProjection), List(ec2Namespace, elbNamespace), None, between, period)
        val Left(planningError) = new Planner().plan(cwQuery)
        planningError should be(UnmatchedProjection(ec2AvgProjection))
      }

      "fail when the same alias is used in different projections" in {
        val ec2AvgProjection = Projection(Statistic("avg"), Some("ec2"), Some("avg_time"), "time")
        val ec2Namespace = Namespace("AWS/EC2", Some("ec2"))
        val elbAvgProjection = Projection(Statistic("avg"), Some("elb"), Some("avg_time"), "latency")
        val elbNamespace = Namespace("AWS/ELB", Some("elb"))
        val between = Between("2018-01-01T00:00:00Z", "2018-01-31T23:59:59Z")
        val period = Period(60)
        val cwQuery = Query(List(ec2AvgProjection, elbAvgProjection), List(ec2Namespace, elbNamespace), None, between, period)
        val Left(planningError) = new Planner().plan(cwQuery)
        planningError should be(ProjectionAliasAlreadyInUse(elbAvgProjection))
      }
    }

    "fail when start time is after end time" in {
      val avgProjection = Projection(Statistic("avg"), None, None, "time")
      val sumProjection = Projection(Statistic("sum"), None, None, "request_size")
      val namespace = Namespace("AWS/EC2", None)
      val between = Between("2018-01-01T10:00:00Z", "2018-01-01T00:00:00Z")
      val period = Period(60)
      val cwQuery = Query(List(avgProjection, sumProjection), List(namespace), None, between, period)
      val Left(planningError) = new Planner().plan(cwQuery)
      planningError should be(StartTimeAfterEndTime)
    }
  }
}
