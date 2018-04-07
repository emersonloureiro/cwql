package cf.janga.cwql.api.planner

import cf.janga.cwql.api.parser._
import cf.janga.cwql.api.planner.CwQueryConversions._
import com.amazonaws.auth.{AWSCredentialsProvider, DefaultAWSCredentialsProviderChain}
import com.amazonaws.services.cloudwatch.model.GetMetricStatisticsRequest
import org.joda.time.format.ISODateTimeFormat

import scala.collection.JavaConverters._

case class QueryPlan(steps: Seq[Step])

private case class GroupedProjections(namespace: String, metric: String, projections: Seq[Projection], selectionOption: Option[Selection])

sealed trait PlannerError
case object StartTimeAfterEndTime extends PlannerError
case class NoMatchingNamespace(projection: Projection) extends PlannerError

class Planner(awsCredentialsProvider: AWSCredentialsProvider = new DefaultAWSCredentialsProviderChain()) {

  def plan(query: Query): Either[PlannerError, QueryPlan] = {
    for {
      projectionsPerMetric <- groupProjectionsPerMetric(query.projections, query.namespaces, query.selectionOption, Map.empty)
      cwRequestStep <- planCwRequestStep(projectionsPerMetric, query.between, query.period, Seq.empty)
    } yield {
      QueryPlan(Seq(cwRequestStep, OrderByStep(None)))
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

  private def groupProjectionsPerMetric(projections: Seq[Projection],
                                        namespaces: Seq[Namespace],
                                        selectionOption: Option[Selection],
                                        groupedProjectionsMap: Map[String, GroupedProjections]): Either[PlannerError, Seq[GroupedProjections]] = projections.headOption match {
    case Some(projection) => {
      val matchingNamespace =
        namespaces.collectFirst {
          case Namespace(namespaceName, Some(namespaceAlias)) if projection.alias.isDefined && projection.alias.get == namespaceAlias => {
            (s"$namespaceName-$namespaceAlias-${projection.metric}", namespaceName)
          }
          case Namespace(namespaceName, None) if projection.alias.isEmpty && namespaces.size == 1 => {
            (s"$namespaceName-${projection.metric}", namespaceName)
          }
        }
      matchingNamespace match {
        case Some((key, namespace)) => {
          groupedProjectionsMap.get(key) match {
            case None => {
              val newMap = groupedProjectionsMap + (key -> GroupedProjections(namespace, projection.metric, Seq(projection), selectionOption))
              groupProjectionsPerMetric(projections.tail, namespaces, selectionOption, newMap)
            }
            case Some(existingGroupedProjections) => {
              val newProjections = existingGroupedProjections.projections ++ Seq(projection)
              val newMap = groupedProjectionsMap + (key -> GroupedProjections(namespace, projection.metric, newProjections, existingGroupedProjections.selectionOption))
              groupProjectionsPerMetric(projections.tail, namespaces, selectionOption, newMap)
            }
          }
        }
        case None => Left(NoMatchingNamespace(projection))
      }
    }
    case None => Right(groupedProjectionsMap.values.toSeq)
  }
}

