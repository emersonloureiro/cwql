package cf.janga.cwql.api.planner

import cf.janga.cwql.api.parser._
import cf.janga.cwql.api.planner.CwQueryConversions._
import com.amazonaws.auth.{AWSCredentialsProvider, DefaultAWSCredentialsProviderChain}
import com.amazonaws.services.cloudwatch.model.{Dimension, GetMetricStatisticsRequest}
import org.joda.time.format.ISODateTimeFormat

import scala.collection.JavaConverters._

case class QueryPlan(steps: Seq[Step])

private case class GroupedProjections(namespace: Namespace, metric: String, projections: Seq[Projection], selectionOption: Option[Selection])

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
          request.setNamespace(groupedProjection.namespace.value)
          request.setPeriod(period.value)
          val (statistics, dimensions) =
            groupedProjection.projections.foldLeft(Seq.empty[String], Seq.empty[Dimension]) {
              case ((foldedStatistics, foldedDimensions), projection) => {
                val newStatistics = foldedStatistics ++ Seq(projection.statistic.toAwsStatistic)
                val dimensions = groupedProjection.selectionOption.fold(Seq.empty[Dimension])(s => getDimensionsFromSelection(groupedProjection.namespace, projection, s))
                val newDimensions = foldedDimensions ++ dimensions
                (newStatistics, newDimensions)
              }
            }
          request.setStatistics(statistics.asJava)
          request.setDimensions(dimensions.asJava)
          planCwRequestStep(groupedProjections.tail, between, period, currentRequests ++ Iterable(request))
        }
      }
      case None => Right(CwRequestStep(awsCredentialsProvider, currentRequests.toSeq))
    }
  }

  private def getDimensionsFromSelection(namespace: Namespace, projection: Projection, selection: Selection): Seq[Dimension] = {
    val baseDimensionOption = selection.booleanExpression.simpleBooleanExpression.toDimension(namespace, projection)
    val dimensions = selection.booleanExpression.nested.foldLeft(Seq(baseDimensionOption)) {
      case (foldedDimensions, (booleanOperator, booleanExpression)) => {
        booleanOperator match {
          case And => foldedDimensions ++ Seq(booleanExpression.toDimension(namespace, projection))
        }
      }
    }
    dimensions.flatten
  }

  private def groupProjectionsPerMetric(projections: Seq[Projection],
                                        namespaces: Seq[Namespace],
                                        selectionOption: Option[Selection],
                                        groupedProjectionsMap: Map[String, GroupedProjections]): Either[PlannerError, Seq[GroupedProjections]] = projections.headOption match {
    case Some(projection) => {
      val matchingNamespace =
        namespaces.collectFirst {
          case namespace: Namespace if namespace.aliasOption.isDefined && projection.alias.isDefined && projection.alias.get == namespace.aliasOption.get => {
            (s"${namespace.value}-${namespace.aliasOption.get}-${projection.metric}", namespace)
          }
          case namespace: Namespace if namespace.aliasOption.isEmpty && projection.alias.isEmpty && namespaces.size == 1 => {
            (s"${namespace.value}-${projection.metric}", namespace)
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

