package cf.janga.cwql.planner

import cf.janga.cwql.parser.{CwQuery, Projection}
import com.amazonaws.auth.{AWSCredentialsProvider, DefaultAWSCredentialsProviderChain}
import com.amazonaws.services.cloudwatch.model.GetMetricStatisticsRequest
import org.joda.time.format.ISODateTimeFormat

import scala.collection.JavaConverters._
import scala.util.{Success, Try}

case class CwQueryPlan(steps: Seq[Step])

class CwqlPlanner(awsCredentialsProvider: AWSCredentialsProvider = new DefaultAWSCredentialsProviderChain()) {

  def plan(cwQuery: CwQuery): Try[CwQueryPlan] = {
    for {
      groupedProjections <- index(cwQuery)
    } yield {
      val requests =
        groupedProjections.map {
          groupedProjection => {
            val request = new GetMetricStatisticsRequest()
            request.setMetricName(groupedProjection.metric)
            val formatter = ISODateTimeFormat.dateTimeNoMillis()
            val startTime = formatter.parseDateTime(cwQuery.between.startTime)
            request.setStartTime(startTime.toDate)
            val endTime = formatter.parseDateTime(cwQuery.between.endTime)
            request.setEndTime(endTime.toDate)
            request.setNamespace(groupedProjection.namespace)
            request.setPeriod(cwQuery.period.value)
            val statistics =
              groupedProjection.projections.foldLeft(List.empty[String]) {
                case (foldedStatistics, projection) => {
                  foldedStatistics ++ Seq(projection.statistic.toAws)
                }
              }
            request.setStatistics(statistics.asJava)
            request
          }
        }
      CwQueryPlan(Seq(CwRequestStep(awsCredentialsProvider, requests)))
    }
  }

  private def index(cwQuery: CwQuery): Try[Seq[GroupedProjections]] = {
    val groupedProjectionsMap =
      cwQuery.namespaces.foldLeft(Map.empty[String, GroupedProjections]) {
        case (groups, namespace) => {
          cwQuery.projections.foldLeft(groups) {
            case (innerGroups, projection) => {
              val key = s"${namespace.value}-${projection.metric}"
              innerGroups.get(key) match {
                case None => {
                  innerGroups + (key -> GroupedProjections(namespace.value, projection.metric, Seq(projection)))
                }
                case Some(existingGroupedProjections) => {
                  val newProjections = existingGroupedProjections.projections ++ Seq(projection)
                  innerGroups + (key -> GroupedProjections(namespace.value, projection.metric, newProjections))
                }
              }
            }
          }
        }
      }
    Success(groupedProjectionsMap.values.toSeq)
  }
}

case class GroupedProjections(namespace: String, metric: String, projections: Seq[Projection])

case class ResultSet(records: Seq[Record])

case class Record(values: Map[String, String] = Map.empty[String, String]) {

  def +(record: Record): Record = {
    Record(values ++ record.values)
  }
}

trait Step {
  def execute(inputOption: Option[ResultSet]): ResultSet
}
