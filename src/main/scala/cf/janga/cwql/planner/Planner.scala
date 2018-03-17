package cf.janga.cwql.planner

import cf.janga.cwql.parser.{Between, CwQuery, Period, Projection}
import com.amazonaws.auth.{AWSCredentialsProvider, DefaultAWSCredentialsProviderChain}
import com.amazonaws.services.cloudwatch.model.GetMetricStatisticsRequest
import org.joda.time.format.ISODateTimeFormat

import scala.collection.JavaConverters._
import scala.util.{Success, Try}

case class CwQueryPlan(steps: Seq[Step])

private case class GroupedProjections(namespace: String, metric: String, projections: Seq[Projection])

class CwqlPlanner(awsCredentialsProvider: AWSCredentialsProvider = new DefaultAWSCredentialsProviderChain()) {

  def plan(cwQuery: CwQuery): Try[CwQueryPlan] = {
    for {
      projectionsPerMetric <- groupProjectionsPerMetric(cwQuery)
      cwRequestStep <- planCwRequestStep(projectionsPerMetric, cwQuery.between, cwQuery.period)
    } yield {
      CwQueryPlan(Seq(cwRequestStep))
    }
  }

  private def planCwRequestStep(groupedProjections: Seq[GroupedProjections], between: Between, period: Period): Try[CwRequestStep] = {
    val requests =
      groupedProjections.map {
        groupedProjection => {
          val request = new GetMetricStatisticsRequest()
          request.setMetricName(groupedProjection.metric)
          val formatter = ISODateTimeFormat.dateTimeNoMillis()
          val startTime = formatter.parseDateTime(between.startTime)
          request.setStartTime(startTime.toDate)
          val endTime = formatter.parseDateTime(between.endTime)
          request.setEndTime(endTime.toDate)
          request.setNamespace(groupedProjection.namespace)
          request.setPeriod(period.value)
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
    Success(CwRequestStep(awsCredentialsProvider, requests))
  }

  private def groupProjectionsPerMetric(cwQuery: CwQuery): Try[Seq[GroupedProjections]] = {
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

