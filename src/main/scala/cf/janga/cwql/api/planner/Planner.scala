package cf.janga.cwql.api.planner

import cf.janga.cwql.api.parser._
import com.amazonaws.auth.{AWSCredentialsProvider, DefaultAWSCredentialsProviderChain}
import com.amazonaws.services.cloudwatch.model.GetMetricStatisticsRequest
import org.joda.time.format.ISODateTimeFormat
import CwQueryConversions._

import scala.collection.JavaConverters._
import scala.util.{Success, Try}

case class QueryPlan(steps: Seq[Step])

private case class GroupedProjections(namespace: String, metric: String, projections: Seq[Projection], selectionOption: Option[Selection])

sealed trait PlannerError
case object StartTimeAfterEndTime extends PlannerError

class Planner(awsCredentialsProvider: AWSCredentialsProvider = new DefaultAWSCredentialsProviderChain()) {

  def plan(query: Query): Either[PlannerError, QueryPlan] = {
    for {
      projectionsPerMetric <- groupProjectionsPerMetric(query)
      cwRequestStep <- planCwRequestStep(projectionsPerMetric, query.between, query.period, Seq.empty)
    } yield {
      QueryPlan(Seq(cwRequestStep))
    }
  }

  private def planCwRequestStep(groupedProjections: Seq[GroupedProjections], between: Between, period: Period,
                                currentRequests: Iterable[GetMetricStatisticsRequest]): Either[PlannerError, CwRequestStep] = {
    groupedProjections.headOption match {
      case Some(groupedProjection) => {
        val request = new GetMetricStatisticsRequest()
        request.setMetricName(groupedProjection.metric)
        val formatter = ISODateTimeFormat.dateTimeNoMillis()
        val startTime = formatter.parseDateTime(between.startTime)
        val endTime = formatter.parseDateTime(between.endTime)
        if (startTime.isAfter(endTime)) {
          Left(StartTimeAfterEndTime)
        } else {
          request.setStartTime(startTime.toDate)
          request.setEndTime(endTime.toDate)
          request.setNamespace(groupedProjection.namespace)
          request.setPeriod(period.value)
          val statistics =
            groupedProjection.projections.foldLeft(List.empty[String]) {
              case (foldedStatistics, projection) => {
                foldedStatistics ++ Seq(projection.statistic.toAwsStatistic)
              }
            }
          request.setStatistics(statistics.asJava)
          val dimensions = groupedProjection.selectionOption.toSeq.map {
            selection => {
              val requiredDimension = selection.booleanExpression.simpleBooleanExpression.toDimension
              selection.booleanExpression.nested.foldLeft(Seq(requiredDimension)) {
                case (foldedDimensions, (booleanOperator, booleanExpression)) => {
                  booleanOperator match {
                    case And => foldedDimensions ++ Seq(booleanExpression.toDimension)
                  }
                }
              }
            }
          }
          request.setDimensions(dimensions.flatten.asJava)
          planCwRequestStep(groupedProjections.tail, between, period, currentRequests ++ Iterable(request))
        }
      }
      case None => Right(CwRequestStep(awsCredentialsProvider, currentRequests.toSeq))
    }
  }

  private def groupProjectionsPerMetric(cwQuery: Query): Either[PlannerError, Seq[GroupedProjections]] = {
    val groupedProjectionsMap =
      cwQuery.namespaces.foldLeft(Map.empty[String, GroupedProjections]) {
        case (groups, namespace) => {
          cwQuery.projections.foldLeft(groups) {
            case (innerGroups, projection) => {
              val key = s"${namespace.value}-${projection.metric}"
              innerGroups.get(key) match {
                case None => {
                  innerGroups + (key -> GroupedProjections(namespace.value, projection.metric, Seq(projection), cwQuery.selectionOption))
                }
                case Some(existingGroupedProjections) => {
                  val newProjections = existingGroupedProjections.projections ++ Seq(projection)
                  innerGroups + (key -> GroupedProjections(namespace.value, projection.metric, newProjections, existingGroupedProjections.selectionOption))
                }
              }
            }
          }
        }
      }
    Right(groupedProjectionsMap.values.toSeq)
  }
}

