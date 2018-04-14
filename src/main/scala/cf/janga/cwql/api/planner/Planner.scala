package cf.janga.cwql.api.planner

import cf.janga.cwql.api.parser._
import cf.janga.cwql.api.planner.CwQueryConversions._
import com.amazonaws.auth.{AWSCredentialsProvider, DefaultAWSCredentialsProviderChain}
import com.amazonaws.services.cloudwatch.model.{Dimension, GetMetricStatisticsRequest}
import org.joda.time.format.ISODateTimeFormat

import scala.collection.JavaConverters._

case class QueryPlan(steps: Seq[Step])

private case class GroupedProjections(namespace: Namespace, metric: String, projections: Seq[Projection])

sealed trait PlannerError
case object StartTimeAfterEndTime extends PlannerError
case class UnmatchedProjection(projection: Projection) extends PlannerError
case class UnmatchedFilter(booleanExpression: SimpleBooleanExpression) extends PlannerError

class Planner(awsCredentialsProvider: AWSCredentialsProvider = new DefaultAWSCredentialsProviderChain()) {

  def plan(query: Query): Either[PlannerError, QueryPlan] = {
    for {
      projectionsPerMetric <- groupProjectionsPerMetric(query.projections, query.namespaces, Map.empty)
      cwRequestStep <- planCwRequestStep(projectionsPerMetric, query.selectionOption, query.between, query.period, Seq.empty)
    } yield {
      QueryPlan(Seq(cwRequestStep, OrderByStep(None)))
    }
  }

  private def planCwRequestStep(groupedProjections: Seq[GroupedProjections], selectionOption: Option[Selection],
                                between: Between, period: Period,
                                currentRequests: Iterable[GetMetricStatisticsRequest]): Either[PlannerError, CwRequestStep] = {
    groupedProjections.headOption match {
      case Some(groupedProjection) => {
        val formatter = ISODateTimeFormat.dateTimeNoMillis()
        val startTime = formatter.parseDateTime(between.startTime)
        val endTime = formatter.parseDateTime(between.endTime)
        if (startTime.isAfter(endTime)) {
          Left(StartTimeAfterEndTime)
        } else {
          val request = new GetMetricStatisticsRequest()
          request.setMetricName(groupedProjection.metric)
          request.setStartTime(startTime.toDate)
          request.setEndTime(endTime.toDate)
          request.setNamespace(groupedProjection.namespace.value)
          request.setPeriod(period.value)
          getStatisticsAndDimensions(groupedProjection.namespace, groupedProjection.projections, selectionOption, Seq.empty, Seq.empty).flatMap {
            case (statistics, dimensions, newSelectionOption) => {
              request.setStatistics(statistics.asJava)
              request.setDimensions(dimensions.asJava)
              planCwRequestStep(groupedProjections.tail, newSelectionOption, between, period, currentRequests ++ Iterable(request))
            }
          }
        }
      }
      case None => {
        selectionOption.fold[Either[PlannerError, CwRequestStep]](Right(CwRequestStep(awsCredentialsProvider, currentRequests.toSeq))) {
          selection => Left(UnmatchedFilter(selection.booleanExpression.simpleBooleanExpression))
        }
      }
    }
  }

  def getStatisticsAndDimensions(namespace: Namespace, projections: Seq[Projection], selectionOption: Option[Selection],
                                 statistics: Seq[String], dimensions: Seq[Dimension]): Either[PlannerError, (Seq[String], Seq[Dimension], Option[Selection])] = projections match {
    case projection :: remainder => {
      val newStatistics = statistics ++ Seq(projection.statistic.toAwsStatistic)
      selectionOption match {
        case Some(selection) => {
          val (newSelectionOption, newDimensions) = getDimensionsFromSelection(namespace, projection, selection)
          getStatisticsAndDimensions(namespace, remainder, newSelectionOption, newStatistics, dimensions ++ newDimensions)
        }
        case None => {
          getStatisticsAndDimensions(namespace, remainder, selectionOption, newStatistics, dimensions)
        }
      }
    }
    case Nil => Right((statistics, dimensions, selectionOption))
  }

  private def getDimensionsFromSelection(namespace: Namespace, projection: Projection, selection: Selection): (Option[Selection], Seq[Dimension]) = {
    def getDimensionsFromSelectionRec(namespace: Namespace, projection: Projection,
                                      booleanExpressions: Seq[(BooleanOperator, SimpleBooleanExpression)],
                                      unmatchedExpressions: Seq[(BooleanOperator, SimpleBooleanExpression)],
                                      dimensions: Seq[Dimension]): (Option[Selection], Seq[Dimension]) = {
      booleanExpressions match {
        case booleanExpression :: remainingExpressions => {
          val dimensionOption = booleanExpression._2.toDimension(namespace, projection)
          dimensionOption match {
            case Some(dimension) => {
              val newSelection = selection.booleanExpression.nested.headOption.map(nested =>  Selection(BooleanExpression(nested._2, selection.booleanExpression.nested.tail)))
              getDimensionsFromSelectionRec(namespace, projection, remainingExpressions, unmatchedExpressions, dimensions ++ Seq(dimension))
            }
            case None => {
              getDimensionsFromSelectionRec(namespace, projection, remainingExpressions, unmatchedExpressions ++ Seq(booleanExpression), dimensions)
            }
          }
        }
        case Nil => {
          val newSelectionOption = unmatchedExpressions.headOption.map(unmatchedExpression => Selection(BooleanExpression(unmatchedExpression._2, unmatchedExpressions.tail)))
          (newSelectionOption, dimensions)
        }
      }
    }
    getDimensionsFromSelectionRec(namespace, projection, Seq((And, selection.booleanExpression.simpleBooleanExpression)) ++ selection.booleanExpression.nested, Seq.empty, Seq.empty)
  }

  private def groupProjectionsPerMetric(projections: Seq[Projection],
                                        namespaces: Seq[Namespace],
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
              val newMap = groupedProjectionsMap + (key -> GroupedProjections(namespace, projection.metric, Seq(projection)))
              groupProjectionsPerMetric(projections.tail, namespaces, newMap)
            }
            case Some(existingGroupedProjections) => {
              val newProjections = existingGroupedProjections.projections ++ Seq(projection)
              val newMap = groupedProjectionsMap + (key -> GroupedProjections(namespace, projection.metric, newProjections))
              groupProjectionsPerMetric(projections.tail, namespaces, newMap)
            }
          }
        }
        case None => Left(UnmatchedProjection(projection))
      }
    }
    case None => Right(groupedProjectionsMap.values.toSeq)
  }
}

