package cf.janga.cwql.planner

import cf.janga.cwql.parser.{CwQuery, CwqlParser}
import com.amazonaws.auth.{AWSCredentialsProvider, DefaultAWSCredentialsProviderChain}
import com.amazonaws.services.cloudwatch.model.GetMetricStatisticsRequest
import org.joda.time.format.ISODateTimeFormat

import scala.collection.JavaConverters._
import scala.util.{Success, Try}

case class CwQueryPlan(steps: Seq[Step])

class CwqlPlanner(awsCredentialsProvider: AWSCredentialsProvider = new DefaultAWSCredentialsProviderChain()) {

  def plan(cwQuery: CwQuery): Try[CwQueryPlan] = {
    val namespace = cwQuery.namespaces.head
    val requests =
      cwQuery.projections.map {
        projection => {
          val request = new GetMetricStatisticsRequest()
          request.setMetricName(projection.metric)
          request.setStatistics(List(projection.statistic.toAws).asJava)
          val formatter = ISODateTimeFormat.dateTimeNoMillis()
          val startTime = formatter.parseDateTime(cwQuery.between.startTime)
          request.setStartTime(startTime.toDate)
          val endTime = formatter.parseDateTime(cwQuery.between.endTime)
          request.setEndTime(endTime.toDate)
          request.setNamespace(namespace.value)
          request.setPeriod(cwQuery.period.value)
          request
        }
      }
    Success(CwQueryPlan(Seq(CwRequestStep(awsCredentialsProvider, requests))))
  }
}

case class ResultSet(records: Seq[Record])

case class Record(values: Map[String, String] = Map.empty[String, String]) {

  def +(record: Record): Record = {
    Record(values ++ record.values)
  }
}

trait Step {
  def execute(inputOption: Option[ResultSet]): ResultSet
}
