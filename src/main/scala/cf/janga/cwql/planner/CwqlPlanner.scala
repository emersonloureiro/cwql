package cf.janga.cwql.planner

import cf.janga.cwql.parser.CwQuery
import com.amazonaws.services.cloudwatch.model.{GetMetricStatisticsRequest, Dimension}
import org.joda.time.{DateTime, DateTimeZone}
import org.joda.time.format.ISODateTimeFormat
import scala.collection.JavaConverters._
import scala.util.{Success, Try}

object CwqlPlanner {

  def plan(cwQuery: CwQuery): Try[CwQueryPlan] = {
    val namespace = cwQuery.namespaces.head
    val requests =
      cwQuery.projections.map {
        projection => {
          val request = new GetMetricStatisticsRequest()
          request.setMetricName(projection.metric)
          request.setStatistics(List(projection.statistic.value).asJava)
          val formatter = ISODateTimeFormat.dateTimeNoMillis()
          val startTime = formatter.parseDateTime(cwQuery.between.startTime)
          request.setStartTime(startTime.toDate())
          val endTime = formatter.parseDateTime(cwQuery.between.endTime)
          request.setEndTime(endTime.toDate())
          request.setNamespace(namespace.value)
          request.setPeriod(cwQuery.period.value)
          request
        }
      }
    Success(CwQueryPlan(Seq(CwRequestStep(requests))))
  }
}

case class CwQueryPlan(steps: Seq[Step[_, _]])

sealed trait Step[I, O] {
  def execute(inputOtion: Option[I]): O
}

case class CwRequestStep(requests: Seq[GetMetricStatisticsRequest]) extends Step[Unit, Unit] {

  import com.amazonaws.services.cloudwatch.AmazonCloudWatchClientBuilder;

  val cwClient = AmazonCloudWatchClientBuilder.defaultClient()

  override def execute(inputOption: Option[Unit]): Unit = {
    requests.map {
      request => {
        val result = cwClient.getMetricStatistics(request)
        result.getDatapoints
      }
    }
    ()
  }
}
